// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import scala.collection.JavaConversions._
import scala.collection.mutable
import au.com.bytecode.opencsv.CSVReader
import com.healthmarketscience.jackcess.{Database, Column}
import org.apache.logging.log4j.LogManager

object DBFiller {
  val logger = LogManager.getLogger(getClass.getName.init)

  val samplePointID = "SamplePointID"

  def apply(csv: CSVReader, db: Database) {
    val lines = csv.readAll
    val header = lines(0)
    validateHeaderFields(header) foreach (throw _)
    val records = lines.tail map { fields =>
      (header zip fields).toMap
    }
    validateParams(records) foreach (throw _)
    validateSamplePointIDs(records, db) foreach (throw _)
    val newRecords = records map convertCSVRecord
    newRecords foreach { rec => logger.debug(rec) }
    addRows(db, newRecords)
    db.flush()
    println(s"Added ${newRecords.length} rows to database")
  }

  def validateHeaderFields(header: Seq[String]): Option[Exception] = {
    if (!header.contains(samplePointID))
      Some(new InvalidInputHeader(s"CSV file is missing '$samplePointID' column"))
    else
      None
  }

  def validateParams(records: Seq[Map[String, String]]): Option[Exception] = {
    val missing = (Set.empty[String] /: records) {
      case (miss, rec) =>
        if (!Tables.analytes.contains(rec("Param"))) miss + rec("Param")
        else miss
    }
    if (!missing.isEmpty)
      Some(new MissingParamConversion(
        ("""|The following 'Param' values in the spreadsheet have no known
            | conversion to an analyte code:
            | """ + missing).stripMargin))
    else
      None
  }

  def validateSamplePointIDs(records: Seq[Map[String, String]], db: Database):
      Option[Exception] = {
    val chemistrySample = "Chemistry SampleInfo"
    val knownPoints =
      (Set.empty[String] /: db.getTable(chemistrySample)) {
        case (points, row) =>
          points + row.get("SamplePoint_ID").toString
      }
    val missing = (Set.empty[String] /: records) {
      case (missing, rec) => {
        rec(samplePointID) match {
          case pt: String => {
            if (!knownPoints.contains(pt))
              missing + pt
            else
              missing
          }
        }
      }
    }
    if (!missing.isEmpty) {
      Some(
        new MissingSamplePointID(
          s"The following sample point IDs are not in the '$chemistrySample' table: ${missing.mkString(",")}"))
    }
    else
      None
  }

  def convertCSVRecord(record: Map[String,String]): Map[String,Any] = {
    val result: mutable.Map[String,Any] = mutable.Map()
    record foreach {
      case ("ReportedND", "ND") => {
        result("SampleValue") = record("LowerLimit")
        result("Symbol") = "<"
      }
      case ("ReportedND", v) =>
        result("SampleValue") = v.toFloat
      case ("SamplePointID", id) => {
        result("SamplePoint_ID") = id
        result("Point_ID") = id.init
      }
      case ("Param", p) => {
        result("Analyte") = Tables.analytes(p)
        if (Tables.method.contains(p))
          result("AnalysisMethod") = Tables.method(p)
      }
      case ("Results_Units", u) =>
        result("Units") = Tables.units.getOrElse(record("Param"), u)
      case _ =>
    }
    result.toMap
  }

  def addRows(db: Database, records: Seq[Map[String,Any]]) {
    val table = db.getTable("MajorChemistry")
    val colNames = table.getColumns.map(_.getName)
    records foreach { rec =>
      val row = colNames map { col =>
        rec.getOrElse(col, null).asInstanceOf[Object] }
      logger.debug(row)
      table.addRow(row:_*)
    }
  }
}
