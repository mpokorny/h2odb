// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import java.util.Date

import org.truffulatree.h2odb.UnitSpec

class CellValueSpec extends UnitSpec {
  "A CellValue" should "haveTypeOf its own type, exclusively" in {
    val cells = List(
        CellString("hello"),
        CellNumeric(3.14),
        CellDate(new Date()),
        CellBoolean(true),
        CellBlank,
        CellFormula("x = y"),
        CellError(18.toByte))

    forAll (cells) { cell =>
      forAll (cells) { cell1 =>
        cell.hasTypeOf(cell1) shouldBe (cell1 == cell)
      }
    }
  }
}
