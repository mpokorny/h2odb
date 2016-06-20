// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import java.util.Date

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

import cats._
import cats.data._
import cats.std.list._
import cats.std.option._
import cats.syntax.option._
import cats.syntax.foldable._
import com.healthmarketscience.jackcess.{Database, Table}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.slf4j.LoggerFactory

object DBFiller {
  private val logger = LoggerFactory.getLogger(getClass.getName.init)

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
    *     names.
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
    Xor.catchOnly[IllegalArgumentException](workbook.getSheetAt(0)).fold(
      _ => writeln("Failed to open worksheet 0 of XLS file: added 0 rows to database"),
      sh => processRows(writeln, db, xls.Table.initial(sh))
    )
  }

  def processRows(
    writeln: (String) => Unit,
    db: Database,
    sheet: TState[SState]): Unit = {

    // get major chemistry table from database
    val majorChemistry = db.getTable(Tables.DbTableInfo.MajorChemistry.name)

    // get minor chemistry table from database
    val minorChemistry = db.getTable(Tables.DbTableInfo.MinorChemistry.name)

    // get chemistry sample info table from database
    val chemSampleInfo = db.getTable(Tables.DbTableInfo.ChemistrySampleInfo.name)

    def getSamples(t: Table): Set[(String,String)] =
      t.foldLeft(Set.empty[(String,String)]) {
        case (acc, row) =>
          val analyte = row.get(Tables.DbTableInfo.analyte)
          if (analyte != null)
            acc + ((row.get(Tables.DbTableInfo.samplePointId).toString,
                    analyte.toString))
          else
            acc
      }

    val existingSamples =
      List(majorChemistry, minorChemistry) map (getSamples _) reduceLeft (_ ++ _)

    implicit val sheetSource = xls.Sheet.source

    val recordSource: AnalysisReport.Source[TState[SState]] = AnalysisReport.source

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
        recAcc => processDbRecords(writeln, recAcc.values))
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
        (info.withFilter(_(DbTableInfo.samplePointId) == record.samplePointId).
           map(_(DbTableInfo.samplePointGUID))).headOption.map(_.toString),
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
    /* FIXME: error messages */
    val messages = errs.unwrap.sortBy(_._1) map { case (_, err) => err.message }

    writeln(messages.mkString("\n"))
  }

  private def processDbRecords(
    writeln: String => Unit,
    records: Iterable[DbRecord]): Unit = {

    writeln(records.take(20).mkString("\n")) // for testing
      /* val output = results.fold (
       *     errs => errs.unwrap map (_.message),
       *     _ match {
       *       case (recs@_, poor@_) =>
       *         showAdditions(recs) ++ Seq("----------") ++ showPoorQuality(poor)
       *     })
       *
       * // filter out records for low priority tests
       * val newRecords = removeLowPriorityRecords(convertedRecords)
       * if (!newRecords.isEmpty) {
       *   if (logger.isDebugEnabled)
       *     newRecords foreach { rec => logger.debug((rec - "Table").toString) }
       *       // add rows to database
       *       addChemTableRows(newRecords)
       *       db.flush()
       *       // report on added records
       *   val sortedRecords = newRecords.sorted
       *     writeln(
       *       s"Added ${newRecords.length} records with the following sample point IDs to database:")
       *       ((Set.empty[String] /: sortedRecords) {
       *          case (acc, rec) => acc + rec(Tables.DbTableInfo.samplePointId).toString
       *        }).toSeq.sorted foreach { id =>
       *         writeln(id)
       *       }
       *     writeln("----------")
       *     // test values against water quality standards
       *     checkStandards(writeln, sortedRecords)
       * } else {
       *   writeln("Added 0 rows to database")
       * } */

  }

  /** Compare analyte test result to water quality standards
    *
    * @param record  [[DbRecord]] to compare to standards
    * @return        true, if test result falls within limits;
    *                false, otherwise
    */
  /* private def meetsStandards(record: DbRecord): Boolean = {
   *   import Tables.DbTableInfo._
   *     (Tables.standards.get(baseAnalyte(record(analyte).toString)) map {
   *        case (lo, hi) => {
   *          record(sampleValue) match {
   *            case v: Float => lo <= v && v <= hi
   *          }
   *        }
   *      }).getOrElse(true)
   * } */

  /** Add records to chemistry database tables
    *
    * Add each record to the appropriate chemistry database table
    *
    * @param records  Seq of [[DbRecord]]s to add to database
    */
  /* private def addChemTableRows(records: Seq[DbRecord]): Unit = {
   *   val tables = Set((records map (_.apply("Table").asInstanceOf[Table])):_*)
   *   val colNames = Map(
   *       (tables.toSeq map { tab => (tab, tab.getColumns.map(_.getName)) }):_*)
   *     records foreach { rec =>
   *       val table = rec("Table").asInstanceOf[Table]
   *       val row = colNames(table) map { col =>
   *           rec.getOrElse(col, null).asInstanceOf[Object] }
   *       if (logger.isDebugEnabled) logger.debug(s"$row -> ${table.getName}")
   *         table.addRow(row:_*)
   *     }
   * } */

  /** Check and report on analyte test results comparison to standards
    *
    * @param writeln   function to output a line a text
    * @param records   Seq of [[DbRecord]]s to check
    */
  /* private def checkStandards(writeln: (String) => Unit, records: Seq[DbRecord]):
   *     Unit = {
   *   import Tables.DbTableInfo.{analyte, samplePointId, sampleValue, units}
   *   val poorQuality = records filter (!meetsStandards(_))
   *   if (!poorQuality.isEmpty) {
   *     val failStr =
   *       if (poorQuality.length > 1)
   *         s"${poorQuality.length} records fail"
   *       else
   *         "1 record fails"
   *           writeln(failStr + " to meet water standards:")
   *           poorQuality foreach { rec =>
   *             (rec(samplePointId), rec(analyte), rec(sampleValue), rec(units)) match {
   *               case (s: String, a: String, v: Float, u: String) =>
   *                 writeln(f"$s - $a ($v%g $u)")
   *             }
   *           }
   *   } else writeln("All records meet all water standards")
   * } */

  /* private def showAdditions(recs: Seq[DbRecord]): Seq[String] = {
   *   val summary =
   *     s"Added ${recs.length} records with the following sample point IDs to database:"
   *
   *   val ids =
   *     recs.foldLeft(Set.empty[String]) {
   *       case (acc, rec) =>
   *         acc + rec(Tables.DbTableInfo.samplePointId).toString
   *     }.toSeq.sorted
   *
   *   summary +: ids
   * } */

  /* private def showPoorQuality(poor: Seq[DbRecord]): Seq[String] = {
   *   import Tables.DbTableInfo.{analyte, samplePointId, sampleValue, units}
   *   if (!poor.isEmpty) {
   *     val failStr =
   *       if (poor.length > 1) s"${poor.length} records fail"
   *       else "1 record fails"
   *
   *     val summary = failStr + " to meet water standards:"
   *
   *     val reports =
   *       poor.sorted map { rec =>
   *         (rec(samplePointId), rec(analyte), rec(sampleValue), rec(units)) match {
   *           case (s: String, a: String, v: Float, u: String) =>
   *             f"$s - $a ($v%g $u)"
   *         }
   *       }
   *
   *     summary +: reports
   *
   *   } else {
   *     Seq("All records meet all water standards")
   *   }
   * } */

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
