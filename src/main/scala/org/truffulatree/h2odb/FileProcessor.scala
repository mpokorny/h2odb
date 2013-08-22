// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import scala.swing._
import scala.collection.JavaConversions._
import scala.collection.mutable
import com.healthmarketscience.jackcess.{Database, Table}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.poi.hssf.usermodel.HSSFWorkbook

object DBFiller {
  private val logger = LoggerFactory.getLogger(getClass.getName.init)

  private val samplePointIdXls = "SamplePointID"

  /** Type of records from XLS format file of water analysis results
    */
  type XlsRecord = Map[String,String]

  /** Type of record that is recorded in the target database
    */
  type DbRecord = Map[String,Any]

  /** Process a xls file of water analysis records, and insert the processed
    * records into a database.
    *
    * The processing steps are as follows:
    *
    *  1. Read in all lines of xls file.
    *  1. Check that header line from xls file has the expected column
    *     names.
    *  1. Create a sequence corresponding to the rows in the xls file of maps
    *     from column title to column value.
    *  1. Check that the "Param" value in each element of the sequence (i.e, an
    *     xls row) is an expected value.
    *  1. Check that the "Test" values, for those "Param"s that have tests, are
    *     expected values.
    *  1. Remove sequence elements with sample point IDs that do not exist in
    *     the database "Chemistry SampleInfo" table.
    *  1. Check that sample point IDs in remaining sequence elements do _not_
    *     exist in major and minor chemistry database tables.
    *  1. Convert the sequence of maps derived from the xls into a new sequence
    *     of maps compatible with the database table schemas.
    *  1. Remove "low priority" test results (this ensures that only the most
    *     preferred test results for those rows with "Test" values get into the
    *     database).
    *  1. Add new rows to the database.
    *  1. Scan sequence of maps that were just inserted into the database to
    *     find those records that fail to meet drinking water standards, and
    *     print out a message for those that fail.
    *
    * @param textArea for text output
    * @param xls      HSSFWorkbook from water analysis report in XLS format
    * @param db       Database for target database
    */
  def apply(textArea: TextArea, xls: HSSFWorkbook, db: Database) {
    // read rows from xls file
    val lines = getXlsRows(xls)
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
    // check for known "Test" field values
    validateTests(records) foreach (throw _)
    // filter for sample point id in db
    val knownPoints =
      (Set.empty[String] /:
        db.getTable(Tables.DbTableInfo.ChemistrySampleInfo.name)) {
        case (points, row) =>
          points + row.get(Tables.DbTableInfo.samplePointId).toString
      }
    val recordsInDb =
      records withFilter (r => knownPoints.contains(r(samplePointIdXls)))
    // get major chemistry table from database
    val majorChemistry = db.getTable(Tables.DbTableInfo.MajorChemistry.name)
    // get minor chemistry table from database
    val minorChemistry = db.getTable(Tables.DbTableInfo.MinorChemistry.name)
    // convert records to db schema compatible format
    val convertedRecords = recordsInDb map (
      r => convertXLSRecord(majorChemistry, minorChemistry, r))
    // check that samples don't already exist in major and minor chem tables
    validateSamples(
      convertedRecords,
      List(majorChemistry, minorChemistry)) foreach (throw _)
    // filter out records for low priority tests
    val newRecords = removeLowPriorityRecords(convertedRecords)
    def appendToTextArea(s: String) {
      textArea.append(s + "\n")
    }
    if (!newRecords.isEmpty) {
      if (logger.isDebugEnabled)
        newRecords foreach { rec => logger.debug((rec - "Table").toString) }
      // add rows to database
      addRows(newRecords)
      db.flush()
      // report on added records
      appendToTextArea(
        s"Added ${newRecords.length} records with the following sample point IDs to database:")
      (Set.empty[String] /: newRecords) {
        case (acc, rec) => acc + rec(Tables.DbTableInfo.samplePointId).toString
      } foreach { id =>
        appendToTextArea(id)
      }
      appendToTextArea("----------")
      // test values against water quality standards
      checkStandards(appendToTextArea _, newRecords)
    } else {
      appendToTextArea("Added 0 rows to database")
    }
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
    if (!header.contains(samplePointIdXls))
      Some(
        new InvalidInputHeader(
          s"XLS file is missing '$samplePointIdXls' column"))
    else None
  }

  /** Validate param fields for all records
    *
    * Compare "Param" field values to list of expected values.
    *
    * @param records  Seq of [[XlsRecord]]s to validate
    * @return         Some(exception) when validation fails;
    *                 None, otherwise
    */
  private def validateParams(records: Seq[XlsRecord]): Option[Exception] = {
    val missing = (Set.empty[String] /: records) {
      case (miss, rec) =>
        if (!Tables.analytes.contains(rec("Param"))) miss + rec("Param")
        else miss
    }
    if (!missing.isEmpty)
      Some(new MissingParamConversion(
        ("""|The following 'Param' values in the spreadsheet have no known
            |conversion to an analyte code:
            |\n""" + missing.mkString("\n")).stripMargin))
    else None
  }

  /** Validate test descriptions for all records
    *
    * Compare "Test" field values to list of expected values
    *
    * @param records  Seq of [[XlsRecord]]s to validate
    * @return         Some(exception) when validation fails; None, otherwise
    */
  private def validateTests(records: Seq[XlsRecord]): Option[Exception] = {
    def isValidTest(rec: Map[String,String]) = {
      val param = rec("Param")
      !Tables.testPriority.contains(param) ||
      Tables.testPriority(param).contains(rec("Test"))
    }
    val invalidTests = records filter (!isValidTest(_))
    if (!invalidTests.isEmpty) {
      val invalid = invalidTests map (
        r => (r("SamplePointID"),r("Param"),r("Test")))
      Some(
        new InvalidTestDescription(
          s"Invalid test descriptions for\n${invalid.mkString("\n")}"))
    } else None
  }

  /** Validate samples by checking whether sample point IDs already exist in given
    * database tables for analytes expected in analysis reports.
    *
    * @param records  Seq of [[DbRecord]]s to validate
    * @param tables   Seq of tables to check for existing sample point IDs
    * @return         Some(exception) when validation fails; None, otherwise
    */
  private def validateSamples(
    records: Seq[DbRecord],
    tables: Seq[Table]): Option[Exception] = {
    def getSamples(t: Table): Set[(String,String)] =
      (Set.empty[(String,String)] /: t) {
        case (acc, row) =>
          acc + ((row.get(Tables.DbTableInfo.samplePointId).toString,
            row.get(Tables.DbTableInfo.analyte).toString))
      }
    val existingSamples = tables map (getSamples _) reduceLeft (_ ++ _)
    val invalidRecords = records filter { r =>
      existingSamples.contains(
        (r(Tables.DbTableInfo.samplePointId).toString,
          r(Tables.DbTableInfo.analyte).toString))
    }
    if (!invalidRecords.isEmpty) {
      val invalidSamplePointIds = (Set.empty[String] /: invalidRecords) {
        case (acc, rec) => acc + rec(Tables.DbTableInfo.samplePointId).toString
      }
      Some(
        new DuplicateSample(
        "Database already contains gen chem data for the following sample points\n" +
          s"${invalidSamplePointIds.mkString("\n")}"))
    } else None
  }

  /** Convert xls records to database table format
    *
    * Convert a (single) [[XlsRecord]] into a [[DbRecord]]. The resulting
    * [[DbRecord]] is ready for addition to the appropriate database table.
    *
    * @param major   "Major chemistry" database table
    * @param minor   "Minor chemistry" database table
    * @param record  [[XlsRecord]] to convert
    * @return        [[DbRecord]] derived from record
    */
  private def convertXLSRecord(major: Table, minor: Table, record: XlsRecord):
      DbRecord = {
    val result: mutable.Map[String,Any] = mutable.Map()
    record foreach {

      // "ND" result value
      case ("ReportedND", "ND") => {
        // set value to lower limit (as Float)
        result(Tables.DbTableInfo.sampleValue) =
          record("LowerLimit").toFloat * record("Dilution").toFloat
        // add "symbol" column value (as String)
        result(Tables.DbTableInfo.symbol) = "<"
      }

      // normal result value
      case ("ReportedND", v) =>
        result(Tables.DbTableInfo.sampleValue) = v.toFloat // as Float

      // sample point id
      case ("SamplePointID", id) => {
        // set sample point id (as String)
        result(Tables.DbTableInfo.samplePointId) = id
        // set point id (as String)
        result(Tables.DbTableInfo.pointId) = id.init
      }

      // water parameter identification
      case ("Param", p) => {
        // analyte code (name)
        result(Tables.DbTableInfo.analyte) = Tables.analytes(p) // as String
        // "AnalysisMethod", if required
        if (Tables.method.contains(p))
          result(Tables.DbTableInfo.analysisMethod) = Tables.method(p) // as String
        // record table this result goes into (as table reference)
        result("Table") = Tables.chemistryTable(p) match {
          case Tables.DbTableInfo.MajorChemistry.name => major
          case Tables.DbTableInfo.MinorChemistry.name => minor
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
        result(Tables.DbTableInfo.units) =
          Tables.units.getOrElse(record("Param"), u)

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
        val key = (rec(Tables.DbTableInfo.samplePointId).asInstanceOf[String],
          rec(Tables.DbTableInfo.analyte).asInstanceOf[String])
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
    (Tables.standards.get(record(Tables.DbTableInfo.analyte).toString) map {
      case (lo, hi) => {
        record(Tables.DbTableInfo.sampleValue) match {
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
    * @param writeln   function to output a line a text
    * @param records   Seq of [[DbRecord]]s to check
    */
  private def checkStandards(writeln: (String) => Unit, records: Seq[DbRecord]) {
    import Tables.DbTableInfo.{samplePointId, analyte, sampleValue, units}
    val poorQuality = records filter (!meetsAllStandards(_))
    if (!poorQuality.isEmpty) {
      if (poorQuality.length > 1)
        writeln(s"${poorQuality.length} records fail to meet drinking water standards:")
      else
        writeln("1 record fails to meet drinking water standards:")
      poorQuality foreach { rec =>
        writeln(s"${rec(samplePointId)} - ${rec(analyte)} (${rec(sampleValue)} ${rec(units)})")
      }
    } else writeln("All records meet all drinking water standards")
  }

  /** Get data rows from XLS file.
    * 
    * Cell values are converted to strings.
    * 
    * @param xls  HSSFWorkbook for XLS input file
    */
  private def getXlsRows(xls: HSSFWorkbook): Seq[Seq[String]] = {
    val sheet = xls.getSheetAt(0)
    (sheet.getFirstRowNum to sheet.getLastRowNum) map { r =>
      sheet.getRow(r)
    } withFilter { row =>
      row != null
    } map { row =>
      (row.getFirstCellNum until row.getLastCellNum) map { c =>
        val cell = row.getCell(c)
        if (cell != null) cell.toString else ""
      }
    }
  }

}
