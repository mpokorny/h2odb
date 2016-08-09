// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.xls

import java.util.Date

/** XLS file cell value representation
  * 
  */
sealed trait CellValue {
  type A
  val value: A
  def hasTypeOf(other: CellValue): Boolean
  def typeDescription: String
}

/** String cell value
  */
final case class CellString(value: String) extends CellValue {
  type A = String
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellString(_) => true
      case _ => false
    }
  def typeDescription: String = "String"
}

/** Numeric cell value
  */
final case class CellNumeric(value: Double) extends CellValue {
  type A = Double
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellNumeric(_) => true
      case _ => false
    }
  def typeDescription: String = "Numeric"
}

/** Date cell value
  */
final case class CellDate(value: Date) extends CellValue {
  type A = Date
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellDate(_) => true
      case _ => false
    }
  def typeDescription: String = "Date"
}

/** Boolean cell value
  */
final case class CellBoolean(value: Boolean) extends CellValue {
  type A = Boolean
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellBoolean(_) => true
      case _ => false
    }
  def typeDescription: String = "Boolean"
}

/** Blank cell value
  */
final case object CellBlank extends CellValue {
  type A = Unit
  val value = ()
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellBlank => true
      case _ => false
    }
  def typeDescription: String = "Blank"
}

/** Formula cell value
  */
final case class CellFormula(value: String) extends CellValue {
  type A = String
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellFormula(_) => true
      case _ => false
    }
  def typeDescription: String = "Formula"
}

/** Error cell value
  */
final case class CellError(value: Byte) extends CellValue {
  type A = Byte
  override def hasTypeOf(other: CellValue): Boolean = other match {
      case CellError(_) => true
      case _ => false
    }
  def typeDescription: String = "Error"
}
