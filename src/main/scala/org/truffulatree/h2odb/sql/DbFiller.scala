// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.Connection

import anorm._
import cats._
import cats.data._
import cats.std.list._
import cats.syntax.option._
import org.truffulatree.h2odb

class DBFiller(implicit val connection: Connection)
    extends h2odb.DBFiller[DbRecord] with Tables {

  /** All (samplePointId, analyte) pairs from major and minor chemistry tables
    */
  protected val existingSamples: Set[(String,String)] = {
    val samplePointIdCol = dbInfo.samplePointId

    val analyteCol = dbInfo.analyte

    val parser =
      SqlParser.str(samplePointIdCol) ~ SqlParser.str(analyteCol) map {
        case (samplePointId@_) ~ (analyte@_) =>
          (samplePointId -> analyte)
      }

    def getSamples(table: String): Set[(String,String)] = {
      val pairs = SQL"""
        SELECT #$samplePointIdCol, #$analyteCol FROM #$table
        WHERE #$analyteCol IS NOT NULL"""
        .as(parser.*)

      pairs.toSet
    }

    List(dbInfo.majorChemistry, dbInfo.minorChemistry).
      map(getSamples _).
      reduceLeft(_ ++ _)
  }

  /** Map from samplePointId to samplePointGUID
    */
  private[this] val guids: Map[String, String] = {
    val samplePointIdCol = dbInfo.samplePointId

    val samplePointGUIDCol = dbInfo.samplePointGUID

    val parser =
      SqlParser.str(samplePointIdCol) ~ SqlParser.str(samplePointGUIDCol) map {
        case (samplePointId@_) ~ (samplePointGUID@_) =>
          (samplePointId -> samplePointGUID)
      }

    val pairs = SQL"""
      SELECT #$samplePointIdCol, #$samplePointGUIDCol
      FROM #${dbInfo.chemistrySampleInfo}"""
      .as(parser.*)

    pairs.toMap
  }

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
          priority = dbPriority,
          samplePointGUID = dbSamplePointGUID,
          samplePointId = record.samplePointId,
          sampleValue = dbSampleValue,
          symbol = dbSymbol,
          table = chemistryTable(record.parameter),
          units = unitsMap.getOrElse(record.parameter, record.units))
    }
  }

  override def addToDb(records: Seq[DbRecord]): Xor[Seq[String], Seq[DbRecord]] = {
    records.groupBy(_.table) foreach { case (table, recs) =>

      val namedParameters = recs map toNamedParameters

      logger.debug(namedParameters.map(_.toString + " -> " + table).mkString("\n"))

      addToTable(table, namedParameters)
    }
  }

  private[this] def addToTable(
    table: String,
    records: Seq[Seq[NamedParameter]]): Unit = {
    val insert = s"""
        INSERT INTO $table (
          ${dbInfo.analysesAgency},
          ${dbInfo.analysisDate},
          ${dbInfo.analysisMethod},
          ${dbInfo.analyte},
          ${dbInfo.labId},
          ${dbInfo.samplePointGUID},
          ${dbInfo.samplePointId},
          ${dbInfo.sampleValue},
          ${dbInfo.symbol},
          ${dbInfo.units})
        VALUES (
          {${dbInfo.analysesAgency}},
          {${dbInfo.analysisDate}},
          {${dbInfo.analysisMethod}},
          {${dbInfo.analyte}},
          {${dbInfo.labId}},
          {${dbInfo.samplePointGUID}},
          {${dbInfo.samplePointId}},
          {${dbInfo.sampleValue}},
          {${dbInfo.symbol}},
          {${dbInfo.units}})"""

    records match {
      case Seq(head@_, tail@_*) =>
        BatchSql(insert, head, tail:_*).execute()

      case Seq() =>
    }
  }


  protected def toNamedParameters(record: DbRecord): Seq[NamedParameter] =
    Seq[NamedParameter](
      dbInfo.analysesAgency -> record.analysesAgency,
      dbInfo.analysisDate -> record.analysisDate,
      dbInfo.analysisMethod -> record.analysisMethod,
      dbInfo.analyte -> record.analyte,
      dbInfo.labId -> record.labId,
      dbInfo.samplePointGUID -> record.samplePointGUID,
      dbInfo.samplePointId -> record.samplePointId,
      dbInfo.sampleValue -> record.sampleValue,
      dbInfo.symbol -> record.symbol,
      dbInfo.units -> dbInfo.units)
}
