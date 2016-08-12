// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.Connection

object TransactionIsolation extends Enumeration {
  val ReadUncommited = Value(Connection.TRANSACTION_READ_UNCOMMITTED)

  val ReadCommitted = Value(Connection.TRANSACTION_READ_COMMITTED)

  val RepeatableRead = Value(Connection.TRANSACTION_REPEATABLE_READ)

  val Serializable = Value(Connection.TRANSACTION_SERIALIZABLE)

  val None = Value(Connection.TRANSACTION_NONE)
}
