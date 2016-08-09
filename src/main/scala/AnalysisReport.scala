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
import org.truffulatree.h2odb.xls._

/** Definitions for reading water analysis report Excel files
  */
object AnalysisReport {

  /** A validated row (record) from a water analysis report
    *
    * The first element of the pair is the line number of the row in the Excel
    * file.
    */
  type Row = (Int, ValidatedNel[Error, AnalysisRecord])

  /** StateT producing a value of type Option[Row]
    *
    * @tparam S state
    */
  type Source[S] = StateT[Option, S, Row]

  /** StateT for an analysis report
    *
    * @tparam S XLS sheet (state)
    */
  def source[S : Sheet.Source]: Source[Table.State[S]] = {
    Table.source[S].map { case (i@_, vmap@_) =>
      val vrec =
        vmap.leftMap(_.map(xlsError)).
          andThen(AnalysisRecord.fromXlsRow(_).leftMap(_.map(recordError)))

      (i, vrec)
    }
  }

  /** Representation of a water analysis report as a state transformer and an
    * initial state.
    */
  final case class Doc[S, A](source: StateT[Option, S, A], initial: S)

  /** [[root.cats.Foldable]] instance for [[Doc]]
    */
  implicit def docFoldable[S]: Foldable[Doc[S, ?]] =
    new Foldable[Doc[S, ?]] {

      override def foldLeft[A, B](fa: Doc[S, A], b: B)(f: (B, A) => B): B = {

        def step(st: StateT[Option, S, A], s: S, b: B): B = {
          st.run(s) map { case (s1@_, a@_) =>
            step(st, s1, f(b, a))
          } getOrElse b
        }

        step(fa.source, fa.initial, b)
      }

      override def foldRight[A, B](
        fa: Doc[S, A],
        lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]):
          Eval[B] = {

        def step(st: StateT[Option, S, A], s: S, lb: Eval[B]): Eval[B] =
          Eval.later(st.run(s)) flatMap { optSA =>
            optSA map { case (s1@_, a@_) =>
              f(a, step(st, s1, lb))
            } getOrElse lb
          }

        step(fa.source, fa.initial, lb)
      }
    }

  /** Analysis report errors
    */
  sealed trait Error

  /** Table.InvalidHeader error analog
    *
    * @see [[xls.Table.InvalidHeader]]
    */
  final case class InvalidHeader(columns: Seq[Int]) extends Error

  /** Table.CellType error analog
    *
    * @see [[xls.Table.CellType]]
    */
  final case class CellType(column: Int, expectedType: String) extends Error

  /** AnalysisReport.MissingField error analog
    *
    * @see [[AnalysisReport.MissingField]]
    */
  final case class MissingField(name: String) extends Error

  /** AnalysisReport.FieldType error analog
    *
    * @see [[AnalysisReport.FieldType]]
    */
  final case class FieldType(name: String) extends Error

  // convert table error to AnalysisReport error
  def xlsError(err: Table.Error): Error = err match {
      case Table.InvalidHeader(columns@_) =>
        InvalidHeader(columns)

      case Table.CellType(col@_, typ@_) =>
        CellType(col, typ)
    }

  // convert record error to AnalysisReport error
  def recordError(err: AnalysisRecord.Error): Error = err match {
      case AnalysisRecord.MissingField(name@_) =>
        MissingField(name)
      case AnalysisRecord.FieldType(name@_)=>
        FieldType(name)
    }

  /** Report "Param" column values
    */
  object Params {
    val alkalinity      = "Alkalinity as CaCO3"
    val aluminum        = "Aluminum"
    val anions          = "Anions total"
    val antimony        = "Antimony 121"
    val arsenic         = "Arsenic"
    val barium          = "Barium"
    val beryllium       = "Beryllium"
    val bicarbonate     = "Bicarbonate (HCO3)"
    val boron           = "Boron 11"
    val bromide         = "Bromide"
    val cadmium         = "Cadmium 111"
    val calcium         = "Calcium"
    val carbonate       = "Carbonate (CO3)"
    val cations         = "Cations total"
    val chloride        = "Chloride"
    val chromium        = "Chromium"
    val cobalt          = "Cobalt"
    val copper          = "Copper 65"
    val fluoride        = "Fluoride"
    val hardness        = "Hardness"
    val iron            = "Iron"
    val lead            = "Lead"
    val lithium         = "Lithium"
    val magnesium       = "Magnesium"
    val manganese       = "Manganese"
    val mercury         = "Mercury"
    val molybdenum      = "Molybdenum 95"
    val nickel          = "Nickel"
    val nitrate         = "Nitrate"
    val nitrite         = "Nitrite"
    val phosphate       = "Ortho Phosphate"
    val percentDiff     = "Percent difference"
    val potassium       = "Potassium"
    val selenium        = "Selenium"
    val siliconDioxide  = "SiO2"
    val silicon         = "Silicon"
    val silver          = "Silver 107"
    val sodium          = "Sodium"
    val conductance     = "Specific Conductance"
    val strontium       = "Strontium"
    val sulfate         = "Sulfate"
    val tds             = "TDS calc"
    val thallium        = "Thallium"
    val thorium         = "Thorium"
    val tin             = "Tin"
    val titanium        = "Titanium"
    val uranium         = "Uranium"
    val vanadium        = "Vanadium"
    val zinc            = "Zinc 66"
    val pH              = "pH"
  }

}
