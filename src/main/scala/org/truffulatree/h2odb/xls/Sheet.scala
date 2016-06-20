// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import cats._
import cats.data._
import cats.std.list._
import cats.std.option._
import cats.syntax.option._
import org.apache.poi.hssf.usermodel.{HSSFRow, HSSFSheet}
import org.apache.poi.ss.usermodel.{Cell, DateUtil}


object Sheet {

  type Source[S] = StateT[Option, S, (Int, Seq[CellValue])]

  private def getCellValues(row: HSSFRow): Seq[CellValue] = {
    (row.getFirstCellNum until row.getLastCellNum) map { c =>
      Option(row.getCell(c)) map { cell =>
        cell.getCellType match {
          case Cell.CELL_TYPE_STRING =>
            CellString(cell.getStringCellValue)
          case Cell.CELL_TYPE_NUMERIC =>
            if (DateUtil.isCellDateFormatted(cell))
              CellDate(cell.getDateCellValue)
            else
              CellNumeric(cell.getNumericCellValue)
          case Cell.CELL_TYPE_BOOLEAN =>
            CellBoolean(cell.getBooleanCellValue)
          case Cell.CELL_TYPE_BLANK =>
            CellBlank
          case Cell.CELL_TYPE_FORMULA =>
            CellFormula(cell.getCellFormula)
          case Cell.CELL_TYPE_ERROR =>
            CellError(cell.getErrorCellValue)
        }
      } getOrElse CellBlank
    }
  }

  final case class State(sheet: HSSFSheet, rowIndex: Int) {

    def nextRow: Option[(State, (Int, Seq[CellValue]))] = {

      implicit val rowOptMonoid = MonoidK[Option].algebra[(Int,HSSFRow)]

      val rowOpt: Eval[Option[(Int, HSSFRow)]] =
        Foldable[List].foldRight(
          (rowIndex to sheet.getLastRowNum).toList,
          Eval.now(none[(Int, HSSFRow)])) {
          case (i@_, eval@_) =>
            Option(sheet.getRow(i)).
              map(r => Eval.now(Option((i, r)))).
              getOrElse(eval)
        }

      rowOpt.value map { case (index@_, row@_) =>
        (State(sheet, index + 1), (index, getCellValues(row)))
      }
    }
  }

  def source: Source[State] =
    StateT(_.nextRow)

  def initial(sheet: HSSFSheet): State =
    State(sheet, sheet.getFirstRowNum)
}
