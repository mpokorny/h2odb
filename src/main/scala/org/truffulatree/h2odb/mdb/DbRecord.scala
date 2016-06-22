// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.mdb

import org.truffulatree.h2odb

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
  table: String,
  units: String) extends h2odb.DbRecord with Tables {

  def get(col: String): Option[Any] = col match {
      case dbInfo.analysesAgency =>
        Some(analysesAgency)
      case dbInfo.analysisDate =>
        Some(analysisDate)
      case dbInfo.analysisMethod =>
        Some(analysisMethod)
      case dbInfo.analyte =>
        Some(analyte)
      case dbInfo.labId =>
        Some(labId)
      case dbInfo.pointId =>
        Some(pointId)
      case dbInfo.samplePointGUID =>
        Some(samplePointGUID)
      case dbInfo.samplePointId =>
        Some(samplePointId)
      case dbInfo.sampleValue =>
        Some(sampleValue)
      case dbInfo.symbol =>
        symbol orElse Some("")
      case dbInfo.units =>
        Some(units)
      case _ =>
        None
    }

  override def toString: String =
      s"""|DbRecord($analysesAgency, $analysisDate, $analysisMethod,
          | $analyte, $labId, $pointId, $priority, $samplePointGUID,
          | $samplePointId, $sampleValue, $symbol, $table,
          | $units)""".stripMargin
}
