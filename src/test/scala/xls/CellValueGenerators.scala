// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import java.util.Date

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

trait CellValueGenerators {

  val genString = for { s <- arbitrary[String] } yield CellString(s)

  val genNumeric = for { f <- arbitrary[Float] } yield CellNumeric(f)

  val genDate = for { d <- arbitrary[Date] } yield CellDate(d)

  val genBoolean = for { b <- arbitrary[Boolean] } yield CellBoolean(b)

  val genBlank = Gen.const(CellBlank)

  val genFormula = for { f <- arbitrary[String] } yield CellFormula(f)

}
