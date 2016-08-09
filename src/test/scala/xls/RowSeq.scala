// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import cats.data.StateT
import cats.std.option._

object RowSeq {

  /** State for testing with the XLS file replaced by a sequence of cell value
    * rows.
    */
  final case class State(rows: IndexedSeq[Seq[CellValue]], rowIndex: Int) {

    def nextRow: Option[(State, (Int, Seq[CellValue]))] =
      if (rowIndex < rows.length) {
        Some((State(rows, rowIndex + 1), (rowIndex, rows(rowIndex))))
      } else {
        None
      }
  }

  def source: Sheet.Source[State] =
    StateT(_.nextRow)

  def initial(rows: IndexedSeq[Seq[CellValue]]): State =
    State(rows, 0)
}
