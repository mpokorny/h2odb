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

abstract class DBFiller[A <: DbRecord] extends Tables {

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

  def getFromWorkbook(writeln: String => Unit, workbook: HSSFWorkbook): Unit = {
    val worksheet =
      Xor.catchOnly[IllegalArgumentException](workbook.getSheetAt(0)).
        leftMap(_ => "Failed to open worksheet 0 of XLS file")

    worksheet.fold(
      err => writeln(err),
      sheet => processRows(writeln, xls.Table.initial(sheet)))
  }

  def processRows(writeln: (String) => Unit, sheet: TState[SState]): Unit = {

    implicit val sheetSource = xls.Sheet.source

    val recordSource: AnalysisReport.Source[TState[SState]] =
      AnalysisReport.source

    val dbRecordSource =
      recordSource map { case (i@_, vrec@_) =>
        /* convert errors to DBFiller Errors */
        (i, vrec.leftMap(_ map(fromAnalysisReportError)))
      } map { case (i@_, vrec@_) =>
          /* validate AnalysisRecord, then convert to A, then validate A */
          vrec.
            andThen(validateAnalysisRecord).
            andThen(convertAnalysisRecord).
            andThen(validateSample(_).toValidatedNel).
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
      fold(
        showValidationErrors(writeln, _),
        recAcc => processDbRecords(writeln, recAcc.values.toSeq))
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

  private[this] def validateSample(record: A): Validated[Error, A] =
    if (!existingSamples.contains((record.samplePointId, record.analyte))) {
      Validated.valid(record)
    } else {
      Validated.invalid(DuplicateSample(record.samplePointId, record.analyte))
    }

  private[this] def showValidationErrors(
    writeln: String => Unit,
    errs: NonEmptyList[(Int, Error)]): Unit = {
    val messages =
      errs.unwrap.sortBy(_._1) map { case (i@_, err@_) =>
        s"ERROR: in XLS file, row $i: " + err.message
      }

    writeln(messages.mkString("\n"))
  }

  private[this] def processDbRecords(
    writeln: String => Unit,
    records: Seq[A]):
      Unit = {

    if (!records.isEmpty) {

      logger.debug(records.mkString("\n"))

      /* add rows to database */
      addToDb(records)

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

  private[this] def meetsStandards(record: A): Boolean = {
    standards.get(dbInfo.baseAnalyte(record.analyte)) map {
      case (lo, hi) => lo <= record.sampleValue && record.sampleValue <= hi
    } getOrElse true
  }

  /** Convert xls records to database table format
    */
  protected def convertAnalysisRecord(record: AnalysisRecord): ValidatedNel[Error, A]

  /** Add records to database
    */
  protected def addToDb(records: Seq[A]): Unit

  sealed trait Error {
    def message: String
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
