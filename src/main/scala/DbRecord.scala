// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

trait DbRecord {

  val analyte: String

  val samplePointId: String

  val sampleValue: Float

  val priority: Int

  val units: String

  def resultId: (String, String) = (analyte, samplePointId)
}
