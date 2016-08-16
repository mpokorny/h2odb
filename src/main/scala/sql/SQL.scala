// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.SQLException
import javax.sql.DataSource

import cats.Show
import cats.data.{NonEmptyList, State, Xor, XorT}
import cats.std.list._

object SQL {
  type Exceptions = NonEmptyList[SQLException]

  type Result[S, A, B] = XorT[State[S, ?], NonEmptyList[A], B]

  type Fun1[S, A, B, C] = B => Result[S, A, C]

  type Fun2[S, A, B, C, D] = B => C => Result[S, A, D]

  def catchEx[A, B](a: => A)(implicit B: ErrorContext[B]): Xor[NonEmptyList[B], A] =
    Xor.catchOnly[SQLException](a).
      leftMap(ex => NonEmptyList(B.fromSQLException(ex)))

  def withConnection[S, E, A](
    dataSource: DataSource)(
    fn: ConnectionRef[S, E] => Result[S, E, A])(
    implicit EE: ErrorContext[E],
    SE: Show[E]):
      Result[S, E, A] = {
    ConnectionRef[S, E](dataSource) flatMap { conn =>
      conn.closeOnCompletion(fn(conn))
    }
  }

  trait ErrorContext[A] {
    def fromSQLException(e: SQLException): A

    def fromSQLExceptions(
      es: NonEmptyList[SQLException]): NonEmptyList[A] =
      es map fromSQLException
  }

  trait Runnable[A] {
    protected def apply[S]: State[S, A]

    def run: A =
      apply[Unit].run(()).value._2
  }
}
