// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.Savepoint

import cats.data.State

final class SavepointRef[S] protected (sp: => Savepoint) {

  type ST[A] = State[S, A]

  def savepoint: ST[Savepoint] = State.pure(sp)
}

object SavepointRef {
  def apply[S](sp: => Savepoint): State[S, SavepointRef[S]] =
    State.pure(new SavepointRef[S](sp))
}

