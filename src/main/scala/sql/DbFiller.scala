// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.{Connection, SQLException}

import scala.language.higherKinds

import anorm._
import cats._
import cats.data._
import cats.std.list._
import cats.syntax.all._
import org.truffulatree.h2odb

class DbFiller[S](
  override protected val existingSamples: Set[(String, String)],
  guids: Map[String, String])(
  implicit val connection: ConnectionRef[S, h2odb.DbFiller.Error],
  errorContext: SQL.ErrorContext[h2odb.DbFiller.Error])
    extends h2odb.DbFiller[DbRecord, State[S, ?]] with Tables {

  import h2odb.DbFiller._

  /** Convert xls records to database table format
    *
    * Convert a (single) [[h2odb.AnalysisRecord]] into a [[DbRecord]]. The
    * resulting [[DbRecord]] is ready for addition to the appropriate database
    * table.
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

  override def addToDb(records: Seq[DbRecord]): DbFiller.Result[S, Seq[DbRecord]] = {
    val inserts =
      records.groupBy(_.table).toList map { case (table, recs) =>

        val namedParameters = recs map toNamedParameters

        logger.
          debug(namedParameters.map(_.toString + " -> " + table).mkString("\n"))

        addToTable(table, namedParameters).map(_ => recs)
      }

    XME.sequence(inserts).map(_.flatten)
  }

  private[this] def addToTable(
    table: String,
    records: Seq[Seq[NamedParameter]]): DbFiller.Result[S, Unit] = {
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
        val batch =
          ConnectionRef.lift0[S, Error, Array[Int]](
            BatchSql(insert, head, tail:_*).execute()(_))

        batch(connection).map(_ => ())

      case Seq() =>
        XorT.fromXor[State[S, ?]](Xor.right(()))
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
      dbInfo.units -> record.units)
}

object DbFiller extends Tables {

  type Error = h2odb.DbFiller.Error

  type Result[S, A] = SQL.Result[S, Error, A]

  /** Create a [[DbFiller]] instance with the provided [[java.sql.Connection]].
    *
    * This method attempts to query several tables in the database, and returns
    * an error as soon as any of those queries fails.
    */
  def apply[S](connection: ConnectionRef[S, Error])(
    implicit errorContext: SQL.ErrorContext[Error]): Result[S, DbFiller[S]] = {

    def existingSamples: Result[S, Set[(String,String)]] = {
      val samplePointIdCol = dbInfo.samplePointId

      val analyteCol = dbInfo.analyte

      val parser =
        SqlParser.str(samplePointIdCol) ~ SqlParser.str(analyteCol) map {
          case (samplePointId@_) ~ (analyte@_) =>
            (samplePointId -> analyte)
        }

      def getSamples(table: String): Result[S, List[(String, String)]] = {
        val query =
          ConnectionRef.lift0[S, Error, List[(String, String)]](
            SQL"""
               SELECT #$samplePointIdCol, #$analyteCol FROM #$table
               WHERE #$analyteCol IS NOT NULL"""
              .as(parser.*)(_))

        query(connection)
      }

      getSamples(dbInfo.majorChemistry) flatMap { s1 =>
        getSamples(dbInfo.minorChemistry) map (s2 => (s1 ++ s2).toSet)
      }

      val M = XorT.xorTMonadError[State[S, ?], NonEmptyList[Error]]

      M.traverse(
        List(dbInfo.majorChemistry, dbInfo.minorChemistry))(
        getSamples).
        map(_.flatten.toSet)
    }

    def guids: Result[S, Map[String, String]] = {
      val samplePointIdCol = dbInfo.samplePointId

      val samplePointGUIDCol = dbInfo.samplePointGUID

      val parser =
        SqlParser.str(samplePointIdCol) ~ SqlParser.str(samplePointGUIDCol) map {
          case (samplePointId@_) ~ (samplePointGUID@_) =>
            (samplePointId -> samplePointGUID)
        }

      val pairs =
        ConnectionRef.lift0[S, Error, List[(String, String)]](
          SQL"""
            SELECT #$samplePointIdCol, #$samplePointGUIDCol
            FROM #${dbInfo.chemistrySampleInfo}"""
            .as(parser.*)(_))

      pairs(connection).map(_.toMap) 
    }

    existingSamples flatMap { es =>
      guids map (gs =>
        new DbFiller(es, gs)(connection, errorContext))
    }
  }

  implicit object ErrorContext extends SQL.ErrorContext[h2odb.DbFiller.Error] {
      override def fromSQLException(e: SQLException): h2odb.DbFiller.Error =
        h2odb.DbFiller.DbError(e)
    }
}
