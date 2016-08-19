// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import cats.data.{OneAnd, StateT, Validated, ValidatedNel}
import cats.std.list._
import cats.std.option._
import cats.syntax.option._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{Inside, Inspectors, OptionValues}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.truffulatree.h2odb.UnitSpec

class TableSpec
    extends UnitSpec
    with Inspectors
    with Inside
    with OptionValues
    with GeneratorDrivenPropertyChecks
    with CellValueGenerators {

  implicit val rowSeqSource = RowSeq.source

  type State = Table.State[RowSeq.State]

  type Row = ValidatedNel[Table.Error, Map[String, CellValue]]

  type Source = StateT[Option, State, (Int, Row)]

  def initial(rows: IndexedSeq[Seq[CellValue]]): State =
    new Table.StateUninitialized(RowSeq.initial(rows))

  def toTable(rows: IndexedSeq[Seq[CellValue]]): IndexedSeq[Row] = {
    def step(src: Source, st: State, acc: IndexedSeq[Row]): IndexedSeq[Row] = {
      src.run(st) map { case (st1@_, (_, row@_)) =>
        step(src, st1, acc :+ row)
      } getOrElse acc
    }

    step(Table.source, initial(rows), IndexedSeq.empty)
  }

  "A Table" should "require string values in header row columns" in {
    val rows =
      IndexedSeq(
        List(
          CellString("first"),
          CellNumeric(12.0),
          CellString("third"),
          CellBoolean(false)))

    val table = Table.source.runA(initial(rows))

    table shouldBe defined

    table.value should matchPattern { case (0, Validated.Invalid(_)) => }
  }

  it should "identify columns of non-string values in header row" in {
    val rows =
      IndexedSeq(
        List(
          CellString("first"),
          CellNumeric(12.0),
          CellString("third"),
          CellBoolean(false),
          CellBlank))

    val table = Table.source.runA(initial(rows))

    inside(table.value) {
      case (_, Validated.Invalid(OneAnd(Table.InvalidHeader(cols@_), Nil))) =>
        cols should contain theSameElementsAs Seq(1, 3, 4)
    }
  }

  it should "complete after an erroneous header" in {
    val rows =
      IndexedSeq(
        List(
          CellString("first"),
          CellNumeric(12.0),
          CellString("third"),
          CellBoolean(false)),
        List(
          CellBoolean(true),
          CellString("hello"),
          CellNumeric(-2.2),
          CellString("goodbye")))

    val afterHeader =
      Table.source.runS(initial(rows)) flatMap (s => Table.source.run(s))

    afterHeader shouldBe empty
  }

  it should "provide a map with header values as keys" in {
    val rows =
      IndexedSeq(
        List(
          CellString("first"),
          CellString("second"),
          CellString("third"),
          CellString("fourth")),
        List(
          CellBoolean(true),
          CellString("hello"),
          CellNumeric(-2.2),
          CellString("world")),
        List(
          CellBoolean(false),
          CellString("goodbye"),
          CellNumeric(2.2),
          CellString("nobody")))

    val tableRows = toTable(rows)

    Inspectors.forAll (tableRows) { row =>
      inside(row) { case Validated.Valid(map@_) =>
        map.keySet should contain only ("first", "second", "third", "fourth")
      }
    }

  }

  it should "identify location and expected type of all cell value column-datatype errors" in {

    val genCellType =
      Gen.oneOf(genString, genNumeric, genDate, genBoolean, genBlank, genFormula)

    def col[T <: CellValue](g: Gen[T], n: Int, errorsIn100: Int): Gen[Seq[CellValue]] = {
      val genCell =
        Gen.frequency(
          (errorsIn100, genCellType),
          (100 - errorsIn100, g))

      Gen.listOfN(n, genCell)
    }

    implicit val arbitraryTable =
      Arbitrary {
        Gen.sized { rows =>
          Gen.nonEmptyContainerOf[IndexedSeq, IndexedSeq[CellValue]](
            genCellType.flatMap(g => col(g, rows, 5).map(_.toIndexedSeq)))
        }
      }

    GeneratorDrivenPropertyChecks.forAll { (cols: IndexedSeq[IndexedSeq[CellValue]]) =>

      def transpose[A](ary: IndexedSeq[IndexedSeq[A]]): IndexedSeq[IndexedSeq[A]] =
        (0 until ary(0).length) map { i =>
          (0 until ary.length) map (j => ary(j)(i))
        }

      val typeMatches =
        transpose(
          cols map { col =>
            col.foldLeft((none[CellValue], IndexedSeq.empty[(Boolean, CellValue)])) {
              case ((None, acc@_), v@_) if !v.hasTypeOf(CellBlank) =>
                (Some(v), acc :+ ((true, v)))

              case ((None, acc@_), _) =>
                (None, acc :+ ((true, CellBlank)))

              case ((t@Some(v0@_), acc@_), v@_) =>
                val typeMatch = v.hasTypeOf(v0) || v.hasTypeOf(CellBlank)
                (t, acc :+ ((typeMatch, v0)))
            }._2
          })

      val rows = transpose(cols)

      val header = (0 until cols.length) map (i => CellString(i.toString))

      val tableRows = toTable(header +: rows).zipWithIndex

      Inspectors.forAll (tableRows) { case (row@_, i@_) =>
        inside(row) {
          case Validated.Valid(_) =>
            typeMatches(i).map(_._1) should contain only (true)

          case Validated.Invalid(errs@_) =>
            val errsList = errs.unwrap

            /* expected type descriptions should match expected */
            Inspectors.forAll (errsList) { err =>
              inside(err) { case Table.CellType(col@_, desc@_) =>
                desc shouldBe typeMatches(i)(col)._2.typeDescription
              }
            }

            /* column indexes should match expected */
            val errCols =
              errsList collect { case Table.CellType(col@_, _) => col }

            (0 until cols.length) map (!errCols.contains(_)) should
              contain theSameElementsInOrderAs (typeMatches(i).map(_._1))
        } 
      }
    }

  }
}
