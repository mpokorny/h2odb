// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import cats.data._
import cats.std.list._
import cats.std.option._
import org.apache.poi.hssf.usermodel.HSSFSheet

object Table {

  type Source[S] =
    StateT[Option, S, (Int, ValidatedNel[Error, Map[String, CellValue]])]

  def validateColTypes(
    template: List[Option[CellValue]],
    row: List[CellValue]):
      (List[Option[CellValue]], ValidatedNel[Error, List[CellValue]]) = {

    def validate(cell: ((Option[CellValue], CellValue), Int)):
        (Option[CellValue], ValidatedNel[Error, CellValue]) = {
      val ((templateValue, cellValue), colIdx) = cell

      templateValue map { setValue =>
        val vCell: ValidatedNel[Error, CellValue] =
          /* allow blank values everywhere to support optional values */
          if (cellValue.hasTypeOf(setValue) || cellValue.hasTypeOf(CellBlank))
            Validated.valid(cellValue)
          else
            Validated.invalidNel(CellType(colIdx, setValue.typeDescription))

        (Some(setValue), vCell)
      } getOrElse {
        // set template for this cell only if cell is not blank
        val setValue: Option[CellValue] = 
          if (cellValue.hasTypeOf(CellBlank)) None
          else Some(cellValue)

        (setValue, Validated.valid(cellValue))
      }
    }

    val cells = template.zipAll(row, None, CellBlank).zipWithIndex

    val (newTemplate, vCells) =
      cells.foldLeft(
        (List.empty[Option[CellValue]],
         Validated.valid[Error, List[CellValue]](List.empty).toValidatedNel)) {
        case ((template@_, vCells@_), cell@_) =>
          val (cTemplate, vCell) = validate(cell)

          (cTemplate :: template, vCell.map(List(_)).combine(vCells))
      }

    (newTemplate.reverse, vCells.map(_.reverse))
  }

  sealed trait State[S] {

    implicit val S: Sheet.Source[S]

    def nextRecord:
        Option[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))]
  }

  final class StateUninitialized[S](source: S)(implicit val S: Sheet.Source[S]) extends State[S] {

    val cellString = CellString("")

    override def nextRecord:
        Option[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] = {

      val next =
        S.run(source) map {
          case (newSrc@_, (i@_, hdr@_)) =>
            val nonStringHdrIndices =
              hdr.zipWithIndex.
                withFilter(h => !h._1.hasTypeOf(cellString)).
                map(_._2)
            if (nonStringHdrIndices.length > 0) {
              val newState: State[S] = new StateDone
              val err: ValidatedNel[Error, Map[String, CellValue]] =
                Validated.invalidNel(InvalidHeader(nonStringHdrIndices))

              Some((newState, (i, err)))
            } else {
              val hdrNames = hdr collect { case CellString(s@_) => s }

              new StateReady(
                newSrc,
                hdrNames,
                List.fill(hdrNames.length)(None)).
                nextRecord
            }

        }

      next getOrElse None
    }
  }

  final class StateReady[S](
    source: S,
    colNames: Seq[String],
    cellTemplate: List[Option[CellValue]])(implicit val S: Sheet.Source[S]) extends State[S] {

    override def nextRecord:
        Option[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] = {

      val next =
        S.run(source) map {
          case (newSrc@_, (i@_, cvs@_)) =>
            val (newTemplate, vcvs) = validateColTypes(cellTemplate, cvs.toList)
            val rval = vcvs.map(colNames.zip(_).toMap)
            val newState: State[S] = new StateReady(newSrc, colNames, newTemplate)

            Some((newState, (i, rval)))
        }

      next getOrElse None
    }
  }

  final class StateDone[S](implicit val S: Sheet.Source[S]) extends State[S]  {

    override def nextRecord:
        Option[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] =
      None
  }

  def source[S: Sheet.Source]: Source[State[S]] =
    StateT(_.nextRecord)

  def initial(sheet: HSSFSheet): State[Sheet.State] = {
    implicit val cellsSource = Sheet.source

    new StateUninitialized(Sheet.initial(sheet))
  }

  sealed trait Error
  final case class InvalidHeader(columns: Seq[Int]) extends Error
  final case class CellType(column: Int, expectedType: String) extends Error
}
