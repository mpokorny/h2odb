// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import org.truffulatree.h2odb

trait Tables extends h2odb.Tables {

  val dbInfo =
    new DbInfo {

      /** Name of "chemistry sample info" table */
      val chemistrySampleInfo = "[Chemistry SampleInfo]"

      /** Name of "major chemistry" table */
      val majorChemistry = "MajorChemistry"

      /** Name of "minor chemistry" table */
      val minorChemistry = "MinorandTraceChemistry"
    }
}
