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
import com.healthmarketscience.jackcess.{Database, Table}
import org.slf4j.{Logger, LoggerFactory}

object DBFiller {
  val logger = LoggerFactory.getLogger(getClass.getName.init)

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
    validateTests(records) foreach (throw _)
    val major = db.getTable(Tables.major)
    val minor = db.getTable(Tables.minor)
    val convertedRecords = records map (r => convertCSVRecord(major, minor, r))
    val newRecords = removeLowPriorityRecords(convertedRecords)
    if (logger.isDebugEnabled)
      newRecords foreach { rec => logger.debug((rec - "Table").toString) }
    addRows(newRecords)
    db.flush()
    println(s"Added ${newRecords.length} rows to database")
    val poorQuality = newRecords filter (!meetsAllStandards(_))
    if (poorQuality.length > 0) {
      if (poorQuality.length > 1)
        println(s"${poorQuality.length} records fail to meet drinking water standards:")
      else
        println("1 record fails to meet drinking water standards:")
      poorQuality foreach { rec =>
        println(s"${rec("SamplePoint_ID")} - ${rec("Analyte")} (${rec("SampleValue")} ${rec("Units")})")
      }
    } else println("All records meet all drinking water standards")
  }

  def validateHeaderFields(header: Seq[String]): Option[Exception] = {
    if (!header.contains(samplePointID))
      Some(new InvalidInputHeader(s"CSV file is missing '$samplePointID' column"))
    else None
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
            |conversion to an analyte code:
            |""" + missing.mkString(",")).stripMargin))
    else None
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
            if (!knownPoints.contains(pt)) missing + pt
            else missing
          }
        }
      }
    }
    if (!missing.isEmpty) {
      Some(
        new MissingSamplePointID(
          ("""|The following sample point IDs are not in the '$chemistrySample'table:
              |""" + missing.mkString(",")).stripMargin))
    } else None
  }

  def validateTests(records: Seq[Map[String,String]]): Option[Exception] = {
    def isValidTest(rec: Map[String,String]) = {
      val param = rec("Param")
      !Tables.testPriority.contains(param) ||
      Tables.testPriority(param).contains(rec("Test"))
    }
    val invalidTests = records filter (!isValidTest(_))
    if (invalidTests.length > 0) {
      val invalid = invalidTests map (
        r => (r("SamplePointID"),r("Param"),r("Test")))
      Some(
        new InvalidTestDescription(
          s"Invalid test descriptions for ${invalid.mkString(", ")}"))
    } else None
  }

  def convertCSVRecord(major: Table, minor: Table, record: Map[String,String]):
      Map[String,Any] = {
    val result: mutable.Map[String,Any] = mutable.Map()
    record foreach {
      case ("ReportedND", "ND") => {
        result("SampleValue") = record("LowerLimit").toFloat
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
        result("Table") = Tables.chemistryTable(p) match {
          case Tables.major => major
          case Tables.minor => minor
        }
        result("Priority") =
          if (Tables.testPriority.contains(p))
            Tables.testPriority(p).indexOf(record("Test"))
          else
            0
      }
      case ("Results_Units", u) =>
        result("Units") = Tables.units.getOrElse(record("Param"), u)
      case _ =>
    }
    result.toMap
  }

  def removeLowPriorityRecords(records: Seq[Map[String,Any]]):
      Seq[Map[String,Any]] =
    ((Map.empty[(String,String),Map[String,Any]] /: records) {
      case (newrecs, rec) => {
        val key = (rec("SamplePoint_ID").asInstanceOf[String],
          rec("Analyte").asInstanceOf[String])
        if (!newrecs.contains(key) ||
          (rec("Priority").asInstanceOf[Int] <
            newrecs(key)("Priority").asInstanceOf[Int]))
          newrecs + ((key, rec))
        else newrecs
      }
    }).values.toSeq

  def meetsAllStandards(record: Map[String, Any]): Boolean = {
    (Tables.standards.get(record("Analyte").toString) map {
      case (lo, hi) => {
        record("SampleValue") match {
          case v: Float => lo <= v && v <= hi
        }
      }
    }).getOrElse(true)
  }

  def addRows(records: Seq[Map[String,Any]]) {
    val tables = Set((records map (_.apply("Table").asInstanceOf[Table])):_*)
    val colNames = Map(
      (tables.toSeq map { tab => (tab, tab.getColumns.map(_.getName)) }):_*)
    records foreach { rec =>
      val table = rec("Table").asInstanceOf[Table]
      val row = colNames(table) map { col =>
        rec.getOrElse(col, null).asInstanceOf[Object] }
      if (logger.isDebugEnabled) logger.debug(s"$row -> ${table.getName})")
      table.addRow(row:_*)
    }
  }
}
