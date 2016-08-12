// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.SQLException

import cats.data.{NonEmptyList, State, Xor, XorT}

object SQL {
  type Exceptions = NonEmptyList[SQLException]

  type Result[S, A] = XorT[State[S, ?], Exceptions, A]

  type Fun1[S, A, B] = A => Result[S, B]

  type Fun2[S, A, B, C] = A => B => Result[S, C]

  def catchEx[A](a: => A): Xor[SQL.Exceptions, A] =
    Xor.catchOnly[SQLException](a).leftMap(NonEmptyList(_))
}
