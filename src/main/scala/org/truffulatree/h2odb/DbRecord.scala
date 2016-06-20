// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
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

  import Tables.DbTableInfo

  def resultId: (String, String) = (analyte, samplePointId)

  def get(col: String): Option[Any] = col match {
      case DbTableInfo.analysesAgency =>
        Some(analysesAgency)
      case DbTableInfo.analysisDate =>
        Some(analysisDate)
      case DbTableInfo.analysisMethod =>
        Some(analysisMethod)
      case DbTableInfo.analyte =>
        Some(analyte)
      case DbTableInfo.labId =>
        Some(labId)
      case DbTableInfo.pointId =>
        Some(pointId)
      case DbTableInfo.samplePointGUID =>
        Some(samplePointGUID)
      case DbTableInfo.samplePointId =>
        Some(samplePointId)
      case DbTableInfo.sampleValue =>
        Some(sampleValue)
      case DbTableInfo.symbol =>
        symbol orElse Some("")
      case DbTableInfo.units =>
        Some(units)
      case _ =>
        None
    }

  override def toString: String =
      s"""|DbRecord($analysesAgency, $analysisDate, $analysisMethod,
          | $analyte, $labId, $pointId, $priority, $samplePointGUID,
          | $samplePointId, $sampleValue, $symbol, ${table.getName},
          | $units)""".stripMargin
}
