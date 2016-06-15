// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import scala.language.higherKinds

import cats._
import cats.data._
import cats.std.list._
import cats.std.option._
import org.apache.poi.hssf.usermodel.HSSFSheet

object Table {

  type Source[S, F[_]] =
    StateT[F, S, (Int, ValidatedNel[Error, Map[String, CellValue]])]

  def validateColTypes(
    template: List[CellValue],
    row: List[CellValue]):
      ValidatedNel[Error, List[CellValue]]= {

    def validate(cell: ((CellValue, CellValue), Int)):
        ValidatedNel[Error, CellValue] = {
      val ((templateValue, cellValue), colIdx) = cell

      if (cellValue.hasTypeOf(templateValue))
        Validated.valid(cellValue)
      else
        Validated.invalidNel(CellType(colIdx, templateValue.typeDescription))
    }

    val cells =
      template.zipAll(row, CellBlank, CellBlank).zipWithIndex

    listInstance.traverseU(cells)(validate)
  }

  sealed trait State[S] {

    implicit val S: Sheet.Source[S]

    def nextRecord[F[_]: MonadCombine]:
        F[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))]
  }

  final class StateUninitialized[S: Sheet.Source](source: S) extends State[S] {

    val cellString = CellString("")

    override def nextRecord[F[_]: MonadCombine]:
        F[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] = {

      implicit val F = implicitly[MonadCombine[F]]

      S.run(source) map {
        case (newSrc@_, (i@_, hdr@_)) =>
          val nonStringHdrIndices =
            hdr.zipWithIndex.
              withFilter(h => !h._1.hasTypeOf(cellString)).
              map(_._2)
          if (nonStringHdrIndices.length > 0) {
            val newState: State[S] = new StateDone()
            val err: ValidatedNel[Error, Map[String, CellValue]] =
              Validated.invalidNel(InvalidHeader(nonStringHdrIndices))

            F.pure((newState, (i, err)))
          } else {
            val hdrNames = hdr collect { case CellString(s@_) => s }

            new StateWithColumnNames(newSrc, hdrNames).nextRecord(F)
          }

      } getOrElse F.empty
    }
  }

  final class StateWithColumnNames[S: Sheet.Source](
    source: S,
    colNames: Seq[String]) extends State[S] {

    override def nextRecord[F[_]: MonadCombine]:
        F[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] = {

      implicit val F = implicitly[MonadCombine[F]]

      S.run(source) map {
        case (newSrc@_, (i@_, cvs@_)) =>
          val (newState: State[S], rval) =
            if (cvs.length != colNames.length) {
              (new StateDone(),
               Validated.invalidNel[Error, Map[String, CellValue]](
                 RowHeaderConflict(colNames.length, cvs.length)))
            } else {
              (new StateReady(newSrc, colNames, cvs.toList),
               Validated.valid[Error, Map[String, CellValue]](
                 colNames.zip(cvs).toMap).toValidatedNel)
            }

          F.pure((newState, (i, rval)))

      } getOrElse F.empty
    }
  }

  final class StateReady[S: Sheet.Source](
    source: S,
    colNames: Seq[String],
    cellTemplate: List[CellValue]) extends State[S] {

    override def nextRecord[F[_]: MonadCombine]:
        F[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] = {

      implicit val F = implicitly[MonadCombine[F]]

      S.run(source) map {
        case (newSrc@_, (i@_, cvs@_)) =>
          val newState: State[S] =
            new StateReady(newSrc, colNames, cellTemplate)
          val rval =
            validateColTypes(cellTemplate, cvs.toList).
              map(cells => colNames.zip(cells).toMap)

          F.pure((newState, (i, rval)))
      } getOrElse F.empty
    }
  }

  final class StateDone[S: Sheet.Source]() extends State[S]  {
    override def nextRecord[F[_]: MonadCombine]:
        F[(State[S], (Int, ValidatedNel[Error, Map[String, CellValue]]))] =
      implicitly[MonadCombine[F]].empty
  }

  def source[S: Sheet.Source, F[_]: MonadCombine]: Source[State[S], F] =
    StateT(_.nextRecord[F])

  def initial(sheet: HSSFSheet): State[Sheet.State] = {
    implicit val cellsSource = Sheet.source

    new StateUninitialized(Sheet.initial(sheet))
  }

  sealed trait Error
  final case class InvalidHeader(columns: Seq[Int]) extends Error
  final case class RowHeaderConflict(headerColumns: Int, rowColumns: Int) extends Error
  final case class CellType(column: Int, expectedType: String) extends Error
}
