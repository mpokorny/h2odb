// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
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

class DBFiller(val db: Database, dbTables: Map[String, Table])
    extends h2odb.DBFiller[DbRecord, Tables.type] {

  val tables = Tables

  val majorChemistry = dbTables(tables.dbInfo.majorChemistry)

  val minorChemistry = dbTables(tables.dbInfo.minorChemistry)

  val chemSampleInfo = dbTables(tables.dbInfo.chemistrySampleInfo)

  /** All (samplePointId, analyte) pairs from major and minor chemistry tables
    */
  protected val existingSamples: Set[(String,String)] = {
    def getSamples(t: Table): Set[(String,String)] =
      t.foldLeft(Set.empty[(String,String)]) {
        case (acc, row) =>
          Option(row.get(tables.dbInfo.analyte)) map { analyte =>
            acc + ((row.get(tables.dbInfo.samplePointId).toString,
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
       row(tables.dbInfo.samplePointId).toString ->
         row(tables.dbInfo.samplePointGUID).toString
    }).toMap

  /** Convert xls records to database table format
    *
    * Convert a (single) [[AnalysisRecord]] into a [[DbRecord]]. The resulting
    * [[DbRecord]] is ready for addition to the appropriate database table.
    *
    * @param major   "Major chemistry" database table
    * @param minor   "Minor chemistry" database table
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

    val dbPointId = record.samplePointId.init

    val vDbSamplePointGUID =
      Validated.fromOption(
        guids.get(record.samplePointId), 
        InvalidSamplePointId(record.samplePointId))

    val dbTable = Tables.chemistryTable(record.parameter)

    val dbPriority =
      Tables.testPriority.get(record.parameter).
        map(_.indexWhere(_.findFirstIn(record.test).isDefined)).
        getOrElse(0)

    val dbUnits = Tables.units.getOrElse(record.parameter, record.units)

    val dbAnalyte =
      if (record.total.filter(_.trim.length > 0).isDefined) {
        tables.dbInfo.totalAnalyte(Tables.analytes(record.parameter))
      } else {
        Tables.analytes(record.parameter)
      }

    val dbAnalysisMethod =
      record.method +
        Tables.method.get(record.parameter).map(", " + _).getOrElse("")

    Apply[ValidatedNel[Error, ?]].map2(
      vDbSampleValue.toValidatedNel,
      vDbSamplePointGUID.toValidatedNel) {
      case (dbSampleValue@_, dbSamplePointGUID@_) =>
        DbRecord(
          analysesAgency = tables.dbInfo.analysesAgencyDefault,
          analysisDate = record.analysisTime,
          analysisMethod = dbAnalysisMethod,
          analyte = dbAnalyte,
          labId = record.sampleNumber,
          pointId = dbPointId,
          priority = dbPriority,
          samplePointGUID = dbSamplePointGUID,
          samplePointId = record.samplePointId,
          sampleValue = dbSampleValue,
          symbol = dbSymbol,
          table = Tables.chemistryTable(record.parameter),
          units = dbUnits)
    }
  }

  override protected def dbFlush(): Unit = db.flush()

  override protected def addToTable(record: DbRecord): Unit = {
    /* TODO: error handling when table lookup fails? */
    val colNames = tableColumns.getOrElse(record.table, Seq.empty)

    val row =
      colNames map (col => record.get(col).getOrElse(null).asInstanceOf[Object])

    logger.debug(s"$row -> ${record.table}")

    dbTables(record.table).addRow(row:_*)
  }
}

object DbFiller {
  def getTables(db: Database): Xor[NonEmptyList[String], Map[String, Table]] = {
    
    def getTable(name: String): Xor[String, Table] =
      Xor.fromOption(
        Option(db.getTable(name)),
        s"Failed to find '${name}' table in database")

    (Apply[ValidatedNel[String, ?]].map3(
      getTable(Tables.dbInfo.majorChemistry).toValidatedNel,
      getTable(Tables.dbInfo.minorChemistry).toValidatedNel,
      getTable(Tables.dbInfo.chemistrySampleInfo).toValidatedNel) {
      case (major, minor, info) =>
        Map(
          Tables.dbInfo.majorChemistry -> major,
          Tables.dbInfo.minorChemistry -> minor,
          Tables.dbInfo.chemistrySampleInfo -> info)
    }).toXor
  }
}
