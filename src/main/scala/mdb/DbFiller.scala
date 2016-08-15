// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.mdb

import scala.collection.JavaConversions._

import cats._
import cats.data._
import cats.std.list._
import cats.syntax.option._
import cats.syntax.foldable._
import com.healthmarketscience.jackcess.{Database, Table}
import org.truffulatree.h2odb

class DbFiller(val db: Database, dbTables: Map[String, Table])
    extends h2odb.DbFiller[DbRecord, Id] with Tables {

  import h2odb.DbFiller._

  val majorChemistry = dbTables(dbInfo.majorChemistry)

  val minorChemistry = dbTables(dbInfo.minorChemistry)

  val chemSampleInfo = dbTables(dbInfo.chemistrySampleInfo)

  /** All (samplePointId, analyte) pairs from major and minor chemistry tables
    */
  protected val existingSamples: Set[(String,String)] = {
    def getSamples(t: Table): Set[(String,String)] =
      t.foldLeft(Set.empty[(String,String)]) {
        case (acc, row) =>
          Option(row.get(dbInfo.analyte)) map { analyte =>
            acc + ((row.get(dbInfo.samplePointId).toString,
                    analyte.toString))
          } getOrElse acc
      }

    List(majorChemistry, minorChemistry) map (getSamples _) reduceLeft (_ ++ _)
  }

  /** Map from major/minor chemistry table name to table column names
    */
  private[this] val tableColumns: Map[String, Seq[String]] =
    (List(majorChemistry, minorChemistry) map { t =>
       t.getName -> t.getColumns.map(_.getName).toSeq
     }).toMap

  /** Map from samplePointId to samplePointGUID
    */
  private[this] val guids: Map[String, String] =
    (chemSampleInfo map { row =>
       row(dbInfo.samplePointId).toString ->
         row(dbInfo.samplePointGUID).toString
     }).toMap

  /** Convert xls records to database table format
    *
    * Convert a (single) [[AnalysisRecord]] into a [[DbRecord]]. The resulting
    * [[DbRecord]] is ready for addition to the appropriate database table.
    *
    * @return        [[DbRecord]] derived from record
    */
  override protected def convertAnalysisRecord(record: h2odb.AnalysisRecord):
      ValidatedNel[Error, DbRecord] = {

    val (vDbSampleValue, dbSymbol) =
      if (record.reportedND != "ND") {
        (Validated.catchOnly[NumberFormatException](record.reportedND.toFloat).
           leftMap(_ => ReportedNDFormat: Error),
         none[String])
      } else {
        val sv =
          Validated.fromOption(
            record.lowerLimit.map(_ * record.dilution),
            MissingLowerLimit: Error)

        (sv, Some("<"))
      }

    val vDbSamplePointGUID =
      Validated.fromOption(
        guids.get(record.samplePointId),
        InvalidSamplePointId(record.samplePointId))

    val dbPriority =
      testPriority.get(record.parameter).
        map(_.indexWhere(_.findFirstIn(record.test).isDefined)).
        getOrElse(0)

    val dbAnalyte =
      if (record.total.filter(_.trim.length > 0).isDefined) {
        dbInfo.totalAnalyte(analytes(record.parameter))
      } else {
        analytes(record.parameter)
      }

    val dbAnalysisMethod =
      record.method +
        methodMap.get(record.parameter).map(", " + _).getOrElse("")

    Apply[ValidatedNel[Error, ?]].map2(
      vDbSampleValue.toValidatedNel,
      vDbSamplePointGUID.toValidatedNel) {
      case (dbSampleValue@_, dbSamplePointGUID@_) =>
        DbRecord(
          analysesAgency = dbInfo.analysesAgencyDefault,
          analysisDate = record.analysisTime,
          analysisMethod = dbAnalysisMethod,
          analyte = dbAnalyte,
          labId = record.sampleNumber,
          pointId = record.samplePointId.init,
          priority = dbPriority,
          samplePointGUID = dbSamplePointGUID,
          samplePointId = record.samplePointId,
          sampleValue = dbSampleValue,
          symbol = dbSymbol,
          table = chemistryTable(record.parameter),
          units = unitsMap.getOrElse(record.parameter, record.units))
    }
  }

  override def addToDb(records: Seq[DbRecord]): XM[Seq[DbRecord]] =
    (records.toList traverseU_ addToTable).
      fold(
        th => XME.raiseError(NonEmptyList(DbError(th))),
        _ => XME.pure({ db.flush(); records }))

  protected def addToTable(record: DbRecord): Xor[Throwable, Unit] = {
    val colNames = tableColumns.getOrElse(record.table, Seq.empty)

    val row =
      colNames map (col => record.get(col).getOrElse(null).asInstanceOf[Object])

    logger.debug(s"$row -> ${record.table}")

    Xor.catchNonFatal(dbTables(record.table).addRow(row:_*))
  }
}

object DbFiller extends Tables {
  def getTables(db: Database): Xor[NonEmptyList[String], Map[String, Table]] = {

    def getTable(name: String): Xor[String, Table] =
      Xor.fromOption(
        Option(db.getTable(name)),
        s"Failed to find '${name}' table in database")

    (Apply[ValidatedNel[String, ?]].map3(
       getTable(dbInfo.majorChemistry).toValidatedNel,
       getTable(dbInfo.minorChemistry).toValidatedNel,
       getTable(dbInfo.chemistrySampleInfo).toValidatedNel) {
       case (major, minor, info) =>
         Map(
           dbInfo.majorChemistry -> major,
           dbInfo.minorChemistry -> minor,
           dbInfo.chemistrySampleInfo -> info)
     }).toXor
  }
}
