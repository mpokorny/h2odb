package org.truffulatree.h2odb

import com.healthmarketscience.jackcess.Table

final case class DbRecord(
  analysesAgency: String,
  analysisDate: java.util.Date,
  analysisMethod: String,
  analyte: String,
  labId: String,
  pointId: String,
  priority: Int,
  samplePointGUID: String,
  samplePointId: String,
  sampleValue: Float,
  symbol: Option[String],
  table: Table,
  units: String) {

  def resultId: (String, String) = (analyte, samplePointId)

  override def toString: String =
      s"""|DbRecord($analysesAgency, $analysisDate, $analysisMethod,
          | $analyte, $labId, $pointId, $priority, $samplePointGUID,
          | $samplePointId, $sampleValue, $symbol, ${table.getName},
          | $units)""".stripMargin
}
