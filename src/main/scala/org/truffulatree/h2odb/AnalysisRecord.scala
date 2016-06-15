// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import java.util.Date

import cats.Apply
import cats.data._
import cats.std.list._
import org.truffulatree.h2odb.xls._

final case class AnalysisRecord(
  parameter: String,
  test: String,
  samplePointId: String,
  reportedND: String,
  method: String,
  total: String,
  units: String,
  sampleNumber: String,
  analysisTime: Date)

object AnalysisRecord {
  def fromXlsRow(row: Map[String, CellValue]):
      ValidatedNel[Error, AnalysisRecord] = {

    def fieldValue[A](name: String)(fn: PartialFunction[CellValue, A]):
        ValidatedNel[Error, A] = {
      val getValue: PartialFunction[CellValue, Xor[Error, A]] =
        (fn andThen Xor.right).
          orElse { case _ => Xor.left(FieldType(name)) }

      Xor.fromOption(row.get(name), MissingField(name)).
        flatMap(getValue).toValidatedNel
    }

    def stringValue(name: String): ValidatedNel[Error, String] =
      fieldValue(name) { case CellString(s@_) => s }

    val vParameter = stringValue("Param")
    val vTest = stringValue("Test")
    val vSamplePointId = stringValue("SamplePointID")
    val vReportedND = stringValue("ReportedND")
    val vMethod = stringValue("Method")
    val vTotal = stringValue("Total")
    val vUnits = stringValue("Results_Units")
    val vSampleNumber = stringValue("SampleNumber")
    val vAnalysisTime = fieldValue("AnalysisTime") { case CellDate(d@_) => d }

    Apply[ValidatedNel[Error, ?]].map9(
      vParameter,
      vTest,
      vSamplePointId,
      vReportedND,
      vMethod,
      vTotal,
      vUnits,
      vSampleNumber,
      vAnalysisTime) {
      case (param@_, test@_, spid@_, rnd@_, mth@_, tot@_, un@_, num@_, at@_) =>
        AnalysisRecord(param, test, spid, rnd, mth, tot, un, num, at)
    }
  }

  sealed trait Error
  final case class MissingField(name: String) extends Error
  final case class FieldType(name: String) extends Error
}
