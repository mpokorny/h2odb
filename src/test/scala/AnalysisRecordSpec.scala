// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import java.util.Date

import cats.data.Validated
import cats.std.list._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.prop.GeneratorDrivenPropertyChecks.PropertyCheckConfig

class AnalysisRecordSpec
    extends UnitSpec
    with Inside
    with OptionValues
    with xls.CellValueGenerators {

  "An AnalysisRecord" should "be properly created from a Table of CellValues" in {

    def genOption[A](nonePercent: Int)(a: A): Gen[Option[A]] =
      Gen.frequency(
        (nonePercent, Gen.const(None)),
        (100 - nonePercent, Gen.const(Some(a))))

    val wrongTypePercent = 5

    def genCellSometimes[A](gen: Gen[xls.CellValue]): Gen[xls.CellValue] =
      /* the following isn't quite right, since the generator for the type of
       * "correct" should be left out of choices when the wrong type is
       * chosen...will just ignore that for now */
      Gen.frequency(
        (wrongTypePercent,
         Gen.oneOf(
           genString,
           genNumeric,
           genDate,
           genBoolean,
           genBlank,
           genFormula)),
        (100 - wrongTypePercent, gen))

    val noneIsInvalidPercent = 5

    val noneIsValidPercent = 50

    val genParameter =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.parameterFieldName, _)))

    val genTest =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.testFieldName, _)))

    val genSamplePointId =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.samplePointIdFieldName, _)))

    val genReportedND =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.reportedNDFieldName, _)))

    val genLowerLimit =
      genCellSometimes(genNumeric).
        flatMap(genOption(noneIsValidPercent)).
        map(_.map((AnalysisRecord.lowerLimitFieldName, _)))

    val genDilution =
      genCellSometimes(genNumeric).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.dilutionFieldName, _)))

    val genMethod =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.methodFieldName, _)))

    val genTotal =
      genCellSometimes(genString).
        flatMap(genOption(noneIsValidPercent)).
        map(_.map((AnalysisRecord.totalFieldName, _)))

    val genUnits =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.unitsFieldName, _)))

    val genSampleNumber =
      genCellSometimes(genString).
        flatMap(genOption(noneIsInvalidPercent)).
        map(_.map((AnalysisRecord.sampleNumberFieldName, _)))

    val genAnalysisTime =
      genCellSometimes(genDate).
        flatMap(genOption(50)).
        map(_.map((AnalysisRecord.analysisTimeFieldName, _)))

    def addToMap(gen: Gen[Option[(String, xls.CellValue)]])(m: Map[String, xls.CellValue]):
        Gen[Map[String, xls.CellValue]] =
      gen.map(_.map(m + _).getOrElse(m))

    implicit val genXlsRow =
      Arbitrary(
        Gen.const(Map.empty[String, xls.CellValue]).
          flatMap(addToMap(genParameter)).
          flatMap(addToMap(genTest)).
          flatMap(addToMap(genSamplePointId)).
          flatMap(addToMap(genReportedND)).
          flatMap(addToMap(genLowerLimit)).
          flatMap(addToMap(genDilution)).
          flatMap(addToMap(genMethod)).
          flatMap(addToMap(genTotal)).
          flatMap(addToMap(genUnits)).
          flatMap(addToMap(genSampleNumber)).
          flatMap(addToMap(genAnalysisTime)))

    val cellString = xls.CellString("")

    val cellNumeric = xls.CellNumeric(0.0)

    val cellDate = xls.CellDate(new Date())

    val expectedCellTypes =
      Map(
        (AnalysisRecord.parameterFieldName -> cellString),
        (AnalysisRecord.testFieldName -> cellString),
        (AnalysisRecord.samplePointIdFieldName -> cellString),
        (AnalysisRecord.reportedNDFieldName -> cellString),
        (AnalysisRecord.lowerLimitFieldName -> cellNumeric),
        (AnalysisRecord.dilutionFieldName -> cellNumeric),
        (AnalysisRecord.methodFieldName -> cellString),
        (AnalysisRecord.totalFieldName -> cellString),
        (AnalysisRecord.unitsFieldName -> cellString),
        (AnalysisRecord.sampleNumberFieldName -> cellString),
        (AnalysisRecord.analysisTimeFieldName -> cellDate))

    implicit val generatorDrivenConfig =
        PropertyCheckConfig(minSuccessful = 1000)

    GeneratorDrivenPropertyChecks.forAll { (row: Map[String, xls.CellValue]) =>

      lazy val noBlanksRow =
        row collect {
          case pair@(_, value@_) if value != xls.CellBlank =>
            pair
        }

      inside (AnalysisRecord.fromXlsRow(row)) {
        case Validated.Valid(rec@_) =>
          noBlanksRow.get(AnalysisRecord.parameterFieldName).value shouldBe
            xls.CellString(rec.parameter)

          noBlanksRow.get(AnalysisRecord.testFieldName).value shouldBe
            xls.CellString(rec.test)

          noBlanksRow.get(AnalysisRecord.samplePointIdFieldName).value shouldBe
            xls.CellString(rec.samplePointId)

          noBlanksRow.get(AnalysisRecord.reportedNDFieldName).value shouldBe
            xls.CellString(rec.reportedND)

          noBlanksRow.get(AnalysisRecord.lowerLimitFieldName) shouldBe
            rec.lowerLimit.map(xls.CellNumeric(_))

          noBlanksRow.get(AnalysisRecord.dilutionFieldName).value shouldBe
            xls.CellNumeric(rec.dilution)

          noBlanksRow.get(AnalysisRecord.methodFieldName).value shouldBe
            xls.CellString(rec.method)

          noBlanksRow.get(AnalysisRecord.totalFieldName) shouldBe
            rec.total.map(xls.CellString)

          noBlanksRow.get(AnalysisRecord.unitsFieldName).value shouldBe
            xls.CellString(rec.units)

          noBlanksRow.get(AnalysisRecord.sampleNumberFieldName).value shouldBe
            xls.CellString(rec.sampleNumber)

          noBlanksRow.get(AnalysisRecord.analysisTimeFieldName) shouldBe
            rec.analysisTime.map(xls.CellDate)

        case Validated.Invalid(errs@_) =>
          val errsList = errs.unwrap

          Inspectors.forAll (errsList) { err =>
            inside(err) {
              case AnalysisRecord.MissingField(str@_) =>
                noBlanksRow.get(str) shouldBe empty

              case AnalysisRecord.FieldType(name@_) =>
                row.get(name).value should be
                  ((c: xls.CellValue) => !c.hasTypeOf(expectedCellTypes(name)))

            }
          }
      }
    }
  }
}
