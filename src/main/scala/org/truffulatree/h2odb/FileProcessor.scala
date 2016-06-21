// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import scala.collection.JavaConversions._

import cats._
import cats.data._
import cats.std.list._
import cats.std.option._
import cats.syntax.option._
import cats.syntax.foldable._
import com.healthmarketscience.jackcess.{Database, Table}
import com.typesafe.scalalogging.Logger
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.slf4j.LoggerFactory

object DBFiller {
  private val logger = Logger(LoggerFactory.getLogger(getClass.getName.init))

  import xls.Table.{State => TState}
  import xls.Sheet.{State => SState}

  type DbRecordAcc = Map[(String, String), DbRecord]

  type ValidationResult = Xor[NonEmptyList[(Int, Error)], DbRecordAcc]

  implicit object DbRecordOrdering extends Ordering[DbRecord] {
    def compare(rec0: DbRecord, rec1: DbRecord): Int = {
      (rec0.samplePointId compare rec1.samplePointId) match {
        case 0 => rec0.analyte compare rec1.analyte
        case cmp@_ => cmp
      }
    }
  }

  /** Process a xls file of water analysis records, and insert the processed
    * records into a database.
    *
    * The processing steps are as follows:
    *
    *  1. Read in all lines of xls file.
    *  1. Check that header line from xls file has the expected column
    *     names.st
    *  1. Create a sequence corresponding to the rows in the xls file of maps
    *     from column title to column value.
    *  1. Check that the "Param" value in each element of the sequence (i.e, an
    *     xls row) is an expected value.
    *  1. Check that the "Test" values, for those "Param"s that have tests, are
    *     expected values.
    *  1. Remove sequence elements with sample point IDs that do not exist in
    *     the database "Chemistry SampleInfo" table.
    *  1. Check that sample point IDs in remaining sequence elements do _not_
    *     exist in major and minor chemistry database tables.
    *  1. Convert the sequence of maps derived from the xls into a new sequence
    *     of maps compatible with the database table schemas.
    *  1. Remove "low priority" test results (this ensures that only the most
    *     preferred test results for those rows with "Test" values get into the
    *     database).
    *  1. Add new rows to the database.
    *  1. Add sample lab ids to the database.
    *  1. Scan sequence of maps that were just inserted into the database to
    *     find those records that fail to meet drinking water standards, and
    *     print out a message for those that fail.
    *
    * @param writeln  write a string to output (with added newline)
    * @param xls      HSSFWorkbook from water analysis report in XLS format
    * @param db       Database for target database
    */
  def apply(writeln: String => Unit, workbook: HSSFWorkbook, db: Database): Unit = {
    def getTable(name: String): Xor[String, Table] =
      Xor.fromOption(
        Option(db.getTable(name)),
        s"Failed to find '${name}' table in database")

    /* get major chemistry table from database */
    val majorChemistry = getTable(Tables.DbTableInfo.MajorChemistry.name)

    /* get minor chemistry table from database */
    val minorChemistry = getTable(Tables.DbTableInfo.MinorChemistry.name)

    /* get chemistry sample info table from database */
    val chemSampleInfo = getTable(Tables.DbTableInfo.ChemistrySampleInfo.name)

    val worksheet =
      Xor.catchOnly[IllegalArgumentException](workbook.getSheetAt(0)).
        leftMap(_ => "Failed to open worksheet 0 of XLS file")

    Apply[ValidatedNel[String, ?]].map4(
      majorChemistry.toValidatedNel,
      minorChemistry.toValidatedNel,
      chemSampleInfo.toValidatedNel,
      worksheet.toValidatedNel) {
      case (major@_, minor@_, info@_, sheet@_) =>
        processRows(writeln, db, major, minor, info, xls.Table.initial(sheet))
    } fold (
      errs => errs.unwrap.map(writeln),
      _ => ())
  }

  def processRows(
    writeln: (String) => Unit,
    db: Database,
    majorChemistry: Table,
    minorChemistry: Table,
    chemSampleInfo: Table,
    sheet: TState[SState]): Unit = {

    def getSamples(t: Table): Set[(String,String)] =
      t.foldLeft(Set.empty[(String,String)]) {
        case (acc, row) =>
          Option(row.get(Tables.DbTableInfo.analyte)) map { analyte =>
            acc + (row.get(Tables.DbTableInfo.samplePointId).toString ->
                     analyte.toString)
          } getOrElse acc
      }

    val existingSamples =
      List(majorChemistry, minorChemistry) map (getSamples _) reduceLeft (_ ++ _)

    val tableColumns =
      (List(majorChemistry, minorChemistry) map { t =>
         t -> t.getColumns.map(_.getName).toSeq
       }).toMap

    implicit val sheetSource = xls.Sheet.source

    val recordSource: AnalysisReport.Source[TState[SState]] =
      AnalysisReport.source

    val dbRecordSource =
      recordSource map { case (i@_, vrec@_) =>
        /* convert errors to DBFiller Errors */
        (i, vrec.leftMap(_ map(fromAnalysisRepordError)))
      } map { case (i@_, vrec@_) =>
          /* validate AnalysisRecord, then convert to DbRecord, then validate DbRecord */
          vrec.
            andThen(validateAnalysisRecord).
            andThen(
              convertAnalysisRecord(
                chemSampleInfo,
                majorChemistry,
                minorChemistry,
                _)).
            andThen(validateSample(existingSamples, _).toValidatedNel).
            leftMap(_.map((i, _)))
      }

    implicit val docFoldable = AnalysisReport.DocFoldable[SState]

    val vDbRecords =
      AnalysisReport.Doc(dbRecordSource, sheet).
        foldRight(
          Eval.now(
            Xor.right[NonEmptyList[(Int, Error)], DbRecordAcc](Map.empty))) {
          case (r@_, evr@_) => evr map (vr => accumulateValidatedDbRecord(vr, r))
        }

    vDbRecords.value.
      fold(
        showValidationErrors(writeln, _),
        recAcc => processDbRecords(db, tableColumns, writeln, recAcc.values.toSeq))
  }

  private def accumulateDbRecord(acc: DbRecordAcc, rec: DbRecord):
      DbRecordAcc = {
    val hiPriorityRec =
      acc.get(rec.resultId) map { prior =>
        /* high priority <=> low "_.priority" value */
        if (rec.priority < prior.priority) rec
        else prior
      } getOrElse rec

    acc + (hiPriorityRec.resultId -> hiPriorityRec)
  }

  private def accumulateValidatedDbRecord(
    vResult: ValidationResult,
    vRecord: ValidatedNel[(Int, Error), DbRecord]): ValidationResult =
    vRecord.fold(
      err => vResult.fold(
        errs => Xor.left(err combine errs),
        _ => Xor.left(err)),
      rec => vResult.fold(
        _ => vResult,
        recs => Xor.right(accumulateDbRecord(recs, rec))))

  private def validateParam(rec: AnalysisRecord):
      Validated[Error, AnalysisRecord] =
    if (Tables.analytes.contains(rec.parameter)) Validated.valid(rec)
    else Validated.invalid(MissingParamConversion(rec.parameter))

  private def validateTest(rec: AnalysisRecord):
      Validated[Error, AnalysisRecord] =
    if (Tables.testPriority.get(rec.parameter).
          map(_.exists(_.findFirstIn(rec.test).isDefined)).
          getOrElse(true))
      Validated.valid(rec)
    else
      Validated.invalid(
        InvalidTestDescription(rec.samplePointId, rec.parameter, rec.test))

  private def validateAnalysisRecord(rec: AnalysisRecord):
      ValidatedNel[Error, AnalysisRecord] = {
    val vTest = validateTest(rec)
    val vParam = validateParam(rec)

    Apply[ValidatedNel[Error, ?]].map2(
      vTest.toValidatedNel,
      vParam.toValidatedNel) {
      case (rect@_, _) => rec
    }
  }

  /** Convert xls records to database table format
    *
    * Convert a (single) [[AnalysisRecord]] into a [[DbRecord]]. The resulting
    * [[DbRecord]] is ready for addition to the appropriate database table.
    *
    * @param major   "Major chemistry" database table
    * @param minor   "Minor chemistry" database table
    * @return        [[DbRecord]] derived from record
    */
  private def convertAnalysisRecord(
    info: Table,
    major: Table,
    minor: Table,
    record: AnalysisRecord): ValidatedNel[Error, DbRecord] = {
    import Tables.DbTableInfo

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
        (info.withFilter(_(DbTableInfo.samplePointId).toString == record.samplePointId).
           map(_(DbTableInfo.samplePointGUID).toString)).headOption,
        InvalidSamplePointId(record.samplePointId))

    val dbTable =
      Tables.chemistryTable(record.parameter) match {
        case DbTableInfo.MajorChemistry.name => major
        case DbTableInfo.MinorChemistry.name => minor
      }

    val dbPriority =
      Tables.testPriority.get(record.parameter).
        map(_.indexWhere(_.findFirstIn(record.test).isDefined)).
        getOrElse(0)

    val dbUnits = Tables.units.getOrElse(record.parameter, record.units)

    val dbAnalyte =
      if (record.total.filter(_.trim.length > 0).isDefined) {
        DbTableInfo.totalAnalyte(Tables.analytes(record.parameter))
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
          analysesAgency = DbTableInfo.analysesAgencyDefault,
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
          table = dbTable,
          units = dbUnits)
    }
  }

  private def validateSample(
    existingSamples: Set[(String, String)],
    record: DbRecord):
      Validated[Error, DbRecord] =
    if (existingSamples.contains((record.samplePointId, record.analyte))) {
      Validated.valid(record)
    } else {
      Validated.invalid(DuplicateSample(record.samplePointId, record.analyte))
    }

  private def showValidationErrors(
    writeln: String => Unit,
    errs: NonEmptyList[(Int, Error)]): Unit = {
    val messages =
      errs.unwrap.sortBy(_._1) map { case (i@_, err@_) =>
        s"ERROR: in XLS file, row $i: " + err.message
      }

    writeln(messages.mkString("\n"))
  }

  private def processDbRecords(
    db: Database,
    tableColumns: Map[Table, Seq[String]],
    writeln: String => Unit,
    records: Seq[DbRecord]): Unit = {

    if (!records.isEmpty) {

      logger.debug(records.mkString("\n"))

      /* add rows to database */
      records foreach addToTable(tableColumns)

      db.flush()

      writeln(
        s"Added ${records.length} records with the following sample point IDs to database:")

      writeln(records.map(_.samplePointId).distinct.sorted.mkString("\n"))

      writeln("----------")

      /* test values against water quality standards */
      val poorQuality = records filterNot meetsStandards

      if (poorQuality.isEmpty) {
        writeln("All records meet water quality standards")
      } else {
        val failStr =
          if (poorQuality.length > 1) s"${poorQuality.length} records fail"
          else "1 record fails"

        writeln(failStr + " to meet water quality standards:")

        val reports =
          poorQuality.sorted map (rec =>
            f"${rec.samplePointId} - ${rec.analyte} (${rec.sampleValue}%g ${rec.units})")

        writeln(reports.mkString("\n"))
      }
    } else {
      writeln("Added 0 rows to database")
    }

  }

  private def meetsStandards(record: DbRecord): Boolean = {
    Tables.standards.get(Tables.DbTableInfo.baseAnalyte(record.analyte)) map {
      case (lo, hi) => lo <= record.sampleValue && record.sampleValue <= hi
    } getOrElse true
  }

  private def addToTable(tableColumns: Map[Table, Seq[String]])(record: DbRecord):
      Unit = {
    /* TODO: error handling when table lookup fails? */
    val colNames = tableColumns.getOrElse(record.table, Seq.empty)

    val row =
      colNames map (col => record.get(col).getOrElse(null).asInstanceOf[Object])

    logger.debug(s"$row -> ${record.table.getName}")

    record.table.addRow(row:_*)
  }

  sealed trait Error {
    def message: String
  }

  final case class InvalidHeader(columns: Seq[Int]) extends Error {
    override def message: String =
      s"Invalid cell values in header row, columns $columns: must be text"
  }

  final case class CellType(column: Int, expectedType: String) extends Error {
    override def message: String =
      s"Cell value in column $column has incorrect type: must be $expectedType"
  }

  final case class MissingField(name: String) extends Error {
    override def message: String =
      s"Field '$name' is missing a value"
  }

  final case class FieldType(name: String) extends Error {
    override def message: String =
      s"Value in field '$name' has the wrong data type"
  }

  final case class MissingParamConversion(param: String) extends Error {
    override def message: String =
      s"Param value '$param' has no known conversion to an analyte code"
  }

  final case class InvalidTestDescription(
    samplePointId: String,
    parameter: String,
    test: String) extends Error {
    override def message: String =
      s"Invalid test description ($samplePointId, $parameter, $test)"
  }

  final case class InvalidSamplePointId(id: String) extends Error {
    override def message: String =
      s"Sample point id '$id' is not in database"
  }

  final case object ReportedNDFormat extends Error {
    override def message: String =
      "Value in 'ReportedND' field has invalid format"
  }

  final case object MissingLowerLimit extends Error {
    override def message: String =
      "'LowerLimit' field value is missing"
  }

  final case class DuplicateSample(
    samplePointId: String,
    analyte: String) extends Error {
    override def message: String =
      s"Sample for ($samplePointId, $analyte) already exists in database"
  }

  def fromAnalysisRepordError(err: AnalysisReport.Error): Error = err match {
      case AnalysisReport.InvalidHeader(columns@_) =>
        InvalidHeader(columns)

      case AnalysisReport.CellType(col@_, typ@_) =>
        CellType(col, typ)

      case AnalysisReport.MissingField(name@_) =>
        MissingField(name)

      case AnalysisReport.FieldType(name@_) =>
        FieldType(name)
    }
}
