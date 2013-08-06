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
  private val logger = LoggerFactory.getLogger(getClass.getName.init)

  private val samplePointID = "SamplePointID"

  /** Type of records from CSV format file of water analysis results
    */
  type CsvRecord = Map[String,String]

  /** Type of record that is recorded in the target database
    */
  type DbRecord = Map[String,Any]

  /** Process a csv file of water analysis records, and insert the processed
    * records into a database.
    *
    * The processing steps are as follows:
    *
    *  1. Read in all lines of csv file.
    *  1. Check that header line from csv file has the expected column
    *     names.
    *  1. Create a sequence corresponding to the rows in the csv file of maps
    *     from column title to column value.
    *  1. Check that the "Param" value in each element of the sequence (i.e, a
    *     csv row) is an expected value.
    *  1. Check that sample point ids in the sequence exist in the database
    *     "Chemistry SampleInfo" table.
    *  1. Check that the "Test" values, for those "Param"s that have tests, are
    *     expected values.
    *  1. Convert the sequence of maps derived from the csv into a new sequence
    *     of maps compatible with the database table schemas.
    *  1. Remove "low priority" test results (this ensures that only the most
    *     preferred test results for those rows with "Test" values get into the
    *     database).
    *  1. Add new rows to the database.
    *  1. Scan sequence of maps that were just inserted into the database to
    *     find those records that fail to meet drinking water standards, and
    *     print out a message for those that fail.
    *
    * @param csv   CSVReader derived from water analysis report in CSV format
    * @param db    Database for target database
    */
  def apply(csv: CSVReader, db: Database) {
    // read lines from csv file
    val lines = csv.readAll
    // extract header (column names)
    val header = lines(0)
    // check that header fields have only what is expected
    validateHeaderFields(header) foreach (throw _)
    // create a sequence of maps (column name -> cell value) from cvs rows
    val records = lines.tail map { fields =>
      (header zip fields).toMap
    }
    // check for known "Param" field values
    validateParams(records) foreach (throw _)
    // check for known sample point id field values
    validateSamplePointIDs(records, db) foreach (throw _)
    // check for known "Test" field values
    validateTests(records) foreach (throw _)
    // get major chemistry table from database
    val major = db.getTable(Tables.major)
    // get minor chemistry table from database
    val minor = db.getTable(Tables.minor)
    // convert records to db schema compatible format
    val convertedRecords = records map (r => convertCSVRecord(major, minor, r))
    // filter out records for low priority tests
    val newRecords = removeLowPriorityRecords(convertedRecords)
    if (logger.isDebugEnabled)
      newRecords foreach { rec => logger.debug((rec - "Table").toString) }
    // add rows to database
    addRows(newRecords)
    db.flush()
    // test values against water quality standards
    checkStandards(newRecords)
  }

  /** Validate header fields
    *
    * Compare header field names to list of expected names.
    *
    * @param header  Seq of header field names to validate
    * @return        Some(exception) when validation fails;
    *                None, otherwise
    */
  private def validateHeaderFields(header: Seq[String]): Option[Exception] = {
    if (!header.contains(samplePointID))
      Some(new InvalidInputHeader(s"CSV file is missing '$samplePointID' column"))
    else None
  }

  /** Validate param fields for all records
    *
    * Compare "Param" field values to list of expected values.
    *
    * @param records  Seq of [[CsvRecord]]s to validate
    * @return         Some(exception) when validation fails;
    *                 None, otherwise
    */
  private def validateParams(records: Seq[CsvRecord]): Option[Exception] = {
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

  /** Validate sample point ids for all records
    *
    * Compare sample point ids to values in the "Chemistry SampleInfo" database
    * table.
    *
    * @param records  Seq of [[CsvRecord]]s to validate
    * @param db       target database
    * @return         Some(exception) when validation fails;
    *                 None, otherwise
    */
  private def validateSamplePointIDs(records: Seq[CsvRecord], db: Database):
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

  /** Validate test descriptions for all records
    *
    * Compare "Test" field values to list of expected values
    *
    * @param records  Seq of [[CsvRecord]]s to validate
    * @return         Some(exception) when validation fails; None, otherwise
    */
  private def validateTests(records: Seq[CsvRecord]): Option[Exception] = {
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

  /** Convert csv records to database table format
    *
    * Convert a (single) [[CsvRecord]] into a [[DbRecord]]. The resulting
    * [[DbRecord]] is ready for addition to the appropriate database table.
    *
    * @param major   "Major chemistry" database table
    * @param minor   "Minor chemistry" database table
    * @param record  [[CsvRecord]] to convert
    * @return        [[DbRecord]] derived from record
    */
  private def convertCSVRecord(major: Table, minor: Table, record: CsvRecord):
      DbRecord = {
    val result: mutable.Map[String,Any] = mutable.Map()
    record foreach {

      // "ND" result value
      case ("ReportedND", "ND") => {
        // set value to lower limit (as Float)
        result("SampleValue") = record("LowerLimit").toFloat
        // add "symbol" column value (as String)
        result("Symbol") = "<"
      }

      // normal result value
      case ("ReportedND", v) =>
        result("SampleValue") = v.toFloat // as Float

      // sample point id
      case ("SamplePointID", id) => {
        // set sample point id (as String)
        result("SamplePoint_ID") = id
        // set point id (as String)
        result("Point_ID") = id.init
      }

      // water parameter identification
      case ("Param", p) => {
        // analyte code (name)
        result("Analyte") = Tables.analytes(p) // as String
        // "AnalysisMethod", if required
        if (Tables.method.contains(p))
          result("AnalysisMethod") = Tables.method(p) // as String
        // record table this result goes into (as table reference)
        result("Table") = Tables.chemistryTable(p) match {
          case Tables.major => major
          case Tables.minor => minor
        }
        // set test result priority value (as Int)
        result("Priority") =
          if (Tables.testPriority.contains(p))
            Tables.testPriority(p).indexOf(record("Test"))
          else
            0
      }

      // test result units (as String); some are converted, some not
      case ("Results_Units", u) =>
        result("Units") = Tables.units.getOrElse(record("Param"), u)

      // drop any other column
      case _ =>
    }
    result.toMap
  }

  /** Remove records from low priority test results
    *
    * For each value of the pair (sample point, analyte) retain only the record
    * with the most preferred test method.
    *
    * @param records  Seq of [[DbRecord]]s
    * @return         Seq of [[DbRecord]]s with only the highest priority test
    *                 results remaining
    */
  private def removeLowPriorityRecords(records: Seq[DbRecord]):
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

  /** Compare analyte test results to water quality standards
    *
    * @param record  Seq of [[DbRecord]]s to compare to standards
    * @return        true, if all test results fall within limits;
    *                false, otherwise
    */
  private def meetsAllStandards(record: DbRecord): Boolean = {
    (Tables.standards.get(record("Analyte").toString) map {
      case (lo, hi) => {
        record("SampleValue") match {
          case v: Float => lo <= v && v <= hi
        }
      }
    }).getOrElse(true)
  }

  /** Add records to database tables
    *
    * Add each record to the appropriate database table
    *
    * @param records  Seq of [[DbRecord]]s to add to database
    */
  private def addRows(records: Seq[DbRecord]) {
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

  /** Check and report on analyte test results comparison to standards
    *
    * @param records  Seq of [[DbRecord]]s to check
    */
  private def checkStandards(records: Seq[DbRecord]) {
    println(s"Added ${records.length} rows to database")
    val poorQuality = records filter (!meetsAllStandards(_))
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
}
