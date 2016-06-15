// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import scala.language.higherKinds

import cats._
import cats.data._
import cats.std.list._
import org.truffulatree.h2odb.xls._

object AnalysisReport {

  type Source[S, F[_]] =
    StateT[F, S, (Int, ValidatedNel[Error, AnalysisRecord])]

  def source[S : Sheet.Source, F[_]: MonadCombine]: Source[Table.State[S], F] = {
    Table.source[S, F].map { case (i@_, vmap@_) =>
      val vrec =
        vmap.leftMap(_.map(xlsError)).
          andThen(AnalysisRecord.fromXlsRow(_).leftMap(_.map(recordError)))

      (i, vrec)
    }
  }

  sealed trait Error
  final case class InvalidHeader(columns: Seq[Int]) extends Error
  final case class RowHeaderConflict(headerColumns: Int, rowColumns: Int) extends Error
  final case class CellType(column: Int, expectedType: String) extends Error
  final case class MissingField(name: String) extends Error
  final case class FieldType(name: String) extends Error

  def xlsError(err: Table.Error): Error = err match {
      case Table.InvalidHeader(columns@_) =>
        InvalidHeader(columns)
      case Table.RowHeaderConflict(hdrCols@_, rowCols@_) =>
        RowHeaderConflict(hdrCols, rowCols)
      case Table.CellType(col@_, typ@_) =>
        CellType(col, typ)
    }

  def recordError(err: AnalysisRecord.Error): Error = err match {
      case AnalysisRecord.MissingField(name@_) =>
        MissingField(name)
      case AnalysisRecord.FieldType(name@_)=>
        FieldType(name)
    }
}
