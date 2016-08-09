// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import cats._
import cats.data._
import cats.std.list._
import cats.std.option._
import cats.syntax.foldable._
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import play.api.Logger

abstract class DbFiller[A <: DbRecord] extends Tables {

  import DbFiller._

  protected val logger = Logger(getClass.getName.init)

  import xls.Table.{State => TState}
  import xls.Sheet.{State => SState}

  type DbRecordAcc = Map[(String, String), A]

  type ValidationResult = Xor[NonEmptyList[(Int, Error)], DbRecordAcc]

  implicit object DbRecordOrdering extends Ordering[A] {
    def compare(rec0: A, rec1: A): Int = {
      (rec0.samplePointId compare rec1.samplePointId) match {
        case 0 => rec0.analyte compare rec1.analyte
        case cmp@_ => cmp
      }
    }
  }

  /** All (samplePointId, analyte) pairs from major and minor chemistry tables
    */
  protected val existingSamples: Set[(String,String)]

  def getFromWorkbook(workbook: HSSFWorkbook):
      Xor[NonEmptyList[Error], Seq[String]] = {
    val worksheet =
      Xor.catchOnly[IllegalArgumentException](workbook.getSheetAt(0)).
        leftMap(_ => NonEmptyList(InvalidSheet(0): Error))

    worksheet.flatMap(sh => processRows(xls.Table.initial(sh)))
  }

  def processRows(sheet: TState[SState]): Xor[NonEmptyList[Error], Seq[String]] = {

    implicit val sheetSource = xls.Sheet.source

    val recordSource: AnalysisReport.Source[TState[SState]] =
      AnalysisReport.source

    val dbRecordSource =
      recordSource map { case (i@_, vrec@_) =>
        /* convert errors to DbFiller Errors */
        (i, vrec.leftMap(_ map(fromAnalysisReportError)))
      } map { case (i@_, vrec@_) =>
          /* validate AnalysisRecord, then convert to A, then validate A */
          vrec.
            andThen(validateAnalysisRecord).
            andThen(convertAnalysisRecord).
            andThen(validateSample).
            leftMap(_.map((i, _)))
      }

    implicit val docFoldable = AnalysisReport.docFoldable[SState]

    val vDbRecords =
      AnalysisReport.Doc(dbRecordSource, sheet).
        foldRight(
          Eval.now(
            Xor.right[NonEmptyList[(Int, Error)], DbRecordAcc](Map.empty))) {
          case (r@_, evr@_) => evr map (vr => accumulateValidatedDbRecord(vr, r))
        }

    vDbRecords.value.
      leftMap(_.map { case (r, e) => XlsValidationError(r, e): Error }).
      flatMap(recAcc => processDbRecords(recAcc.values.toSeq))
  }

  private[this] def accumulateDbRecord(acc: DbRecordAcc, rec: A): DbRecordAcc = {
    val hiPriorityRec =
      acc.get(rec.resultId) map { prior =>
        /* high priority <=> low "_.priority" value */
        if (rec.priority < prior.priority) rec
        else prior
      } getOrElse rec

    acc + (hiPriorityRec.resultId -> hiPriorityRec)
  }

  private[this] def accumulateValidatedDbRecord(
    vResult: ValidationResult,
    vRecord: ValidatedNel[(Int, Error), A]): ValidationResult =
    vRecord.fold(
      err => vResult.fold(
        errs => Xor.left(err combine errs),
        _ => Xor.left(err)),
      rec => vResult.fold(
        _ => vResult,
        recs => Xor.right(accumulateDbRecord(recs, rec))))

  private[this] def validateParam(rec: AnalysisRecord):
      Validated[Error, AnalysisRecord] =
    if (analytes.contains(rec.parameter)) Validated.valid(rec)
    else Validated.invalid(MissingParamConversion(rec.parameter))

  private[this] def validateTest(rec: AnalysisRecord):
      Validated[Error, AnalysisRecord] =
    if (testPriority.get(rec.parameter).
          map(_.exists(_.findFirstIn(rec.test).isDefined)).
          getOrElse(true)) {
      Validated.valid(rec)
    } else {
      Validated.invalid(
        InvalidTestDescription(rec.samplePointId, rec.parameter, rec.test))
    }

  private[this] def validateAnalysisRecord(rec: AnalysisRecord):
      ValidatedNel[Error, AnalysisRecord] = {
    val vTest = validateTest(rec)
    val vParam = validateParam(rec)

    Apply[ValidatedNel[Error, ?]].map2(
      vTest.toValidatedNel,
      vParam.toValidatedNel) {
      case (rect@_, _) => rec
    }
  }

  private[this] def validateSample(record: A): ValidatedNel[Error, A] =
    if (!existingSamples.contains((record.samplePointId, record.analyte))) {
      Validated.valid(record)
    } else {
      Validated.invalid(
        NonEmptyList(DuplicateSample(record.samplePointId, record.analyte)))
    }

  private[this] def showValidationErrors(
    errs: NonEmptyList[(Int, Error)]): Seq[String] =
    errs.unwrap.sortBy(_._1) map { case (i@_, err@_) =>
      s"ERROR: in XLS file, row $i: " + err.message
    }

  private[this] def processDbRecords(records: Seq[A]):
      Xor[NonEmptyList[Error], Seq[String]] = {

    if (!records.isEmpty) {

      logger.debug(records.mkString("\n"))

      /* add rows to database */
      addToDb(records).bimap(
        err => NonEmptyList(err),
        recs => (showAdded(recs) :+ "----------") ++ showQuality(recs))

    } else {
      Xor.right(Seq("Added 0 rows to database"))
    }

  }

  private[this] def showAdded(records: Seq[A]): Seq[String] = {
    s"Added ${records.length} records with the following sample point IDs to database:" +:
      records.map(_.samplePointId).distinct.sorted
  }

  private[this] def showQuality(records: Seq[A]): Seq[String] = {
    /* test values against water quality standards */
    val poorQuality = records filterNot meetsStandards

    if (poorQuality.isEmpty) {
      Seq("All records meet water quality standards")
    } else {
      val failStr =
        if (poorQuality.length > 1) s"${poorQuality.length} records fail"
        else "1 record fails"

      val failures =
        poorQuality.sorted map(rec =>
          f"${rec.samplePointId} - ${rec.analyte} (${rec.sampleValue}%g ${rec.units})")

      (failStr + " to meet water quality standards:") +: failures
    }
  }

  private[this] def meetsStandards(record: A): Boolean = {
    standards.get(dbInfo.baseAnalyte(record.analyte)) map {
      case (lo, hi) => lo <= record.sampleValue && record.sampleValue <= hi
    } getOrElse true
  }

  /** Convert xls records to database table format
    */
  protected def convertAnalysisRecord(record: AnalysisRecord): ValidatedNel[Error, A]

  /** Add records to database
    *
    * Records should stop being inserted as soon as an error occurs; therefore,
    * only one error value may be returned.
    */
  protected def addToDb(records: Seq[A]): Xor[DbError, Seq[A]]

  def fromAnalysisReportError(err: AnalysisReport.Error): Error = err match {
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

object DbFiller {

  trait Error {
    def message: String
  }

  case class InvalidSheet(sheet: Int) extends Error {
    override def message: String =
      s"Failed to open worksheet $sheet of XLS file"
  }

  case class InvalidHeader(columns: Seq[Int]) extends Error {
    override def message: String =
      s"Invalid cell values in header row, columns $columns: must be text"
  }

  case class CellType(column: Int, expectedType: String) extends Error {
    override def message: String =
      s"Cell value in column $column has incorrect type: must be $expectedType"
  }

  case class MissingField(name: String) extends Error {
    override def message: String =
      s"Field '$name' is missing a value"
  }

  case class FieldType(name: String) extends Error {
    override def message: String =
      s"Value in field '$name' has the wrong data type"
  }

  case class MissingParamConversion(param: String) extends Error {
    override def message: String =
      s"Param value '$param' has no known conversion to an analyte code"
  }

  case class InvalidTestDescription(
    samplePointId: String,
    parameter: String,
    test: String) extends Error {
    override def message: String =
      s"Invalid test description ($samplePointId, $parameter, $test)"
  }

  case class InvalidSamplePointId(id: String) extends Error {
    override def message: String =
      s"Sample point id '$id' is not in database"
  }

  case object ReportedNDFormat extends Error {
    override def message: String =
      "Value in 'ReportedND' field has invalid format"
  }

  case object MissingLowerLimit extends Error {
    override def message: String =
      "'LowerLimit' field value is missing"
  }

  case class DuplicateSample(
    samplePointId: String,
    analyte: String) extends Error {
    override def message: String =
      s"Sample for ($samplePointId, $analyte) already exists in database"
  }

  case class XlsValidationError(row: Int, err: Error) extends Error {
    override def message: String =
      s"ERROR: in XLS file, row $row: ${err.message}"
  }

  case class DbError(th: Throwable) extends Error {
    override def message: String =
      s"ERROR: Database error: ${th.getMessage}"
  }
}

