// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.{Connection, Savepoint, SQLException}

import scala.concurrent.duration.Duration

import cats.data.{NonEmptyList, State, Xor, XorT}
import cats.std.list._
import cats.syntax.all._

final class ConnectionRef[S] protected (conn: => Xor[SQL.Exceptions, Connection]) {
  self =>

  type ST[A] = State[S, A]

  type Result[A] = SQL.Result[S, A]

  type Fun1[A, B] = SQL.Fun1[S, A, B]

  val connection: Result[Connection] = XorT.fromXor[ST](conn)

  private[this] def apply0[A](conn: Connection => A) =
    ConnectionRef.lift0(conn)(self)

  private[this] def apply1[A, B](conn: Connection => A => B) =
    ConnectionRef.lift1(conn)(self)

  def close: Result[Unit] =
    apply0(_.close())

  def commit: Result[Unit] =
    apply0(_.commit())

  def isClosed: Result[Boolean] =
    apply0(_.isClosed)

  def isReadOnly: Result[Boolean] =
    apply0(_.isReadOnly)

  def isValid: Fun1[Duration, Boolean] =
    apply1(c => (d: Duration) => c.isValid(d.toSeconds.toInt.abs))

  def rollback: Fun1[Option[SavepointRef[S]], Unit] = {
    val spRef2sp: Fun1[Option[SavepointRef[S]], Option[Savepoint]] = { ospr =>
      XorT.right[ST, SQL.Exceptions, Option[Savepoint]](
        ospr.map(spr => spr.savepoint.map(_.some)).
          getOrElse(State.pure(none[Savepoint])))
    }

    val rb: Fun1[Option[Savepoint], Unit] =
      apply1 { c => (osp: Option[Savepoint]) =>
        osp.map(sp => c.rollback(sp)).getOrElse(c.rollback())
      }

    optSpRef => spRef2sp(optSpRef) flatMap rb
  }

  def setAutoCommit: Fun1[Boolean, Unit] =
    apply1(c => (b: Boolean) => c.setAutoCommit(b))

  def setReadOnly: Fun1[Boolean, Unit] =
    apply1(c => (b: Boolean) => c.setReadOnly(b))

  def setSavepoint: Fun1[Option[String], SavepointRef[S]] = {
    val setSp: Fun1[Option[String], Savepoint] =
      apply1 { c => (optName: Option[String]) =>
        optName.map(nm => c.setSavepoint(nm)).getOrElse(c.setSavepoint())
      }

    val spRef: Fun1[Savepoint, SavepointRef[S]] =
      sp => XorT.right[ST, SQL.Exceptions, SavepointRef[S]](SavepointRef(sp))

    optName => setSp(optName) flatMap spRef
  }

  def releaseSavepoint: Fun1[SavepointRef[S], Unit] = { spr =>
    XorT.right[ST, SQL.Exceptions, Savepoint](spr.savepoint).
      flatMap(apply1(c => (sp: Savepoint) => c.releaseSavepoint(sp)))
  }

  def rollbackOnException[A](savepointName: Option[String])(a: Result[A]):
      Result[A] = {
    setSavepoint(savepointName) flatMap { spr =>
      val result =
        a.swap.flatMap(
          ex => XorT.right[ST, A, SQL.Exceptions](
            rollback(Some(spr)).
              fold(ex1 => ex combine ex1, _ => ex))).swap

      releaseSavepoint(spr) // won't return any exceptions here

      result
    }
  }

  def commitOnSuccess[A](savepointName: Option[String])(a: Result[A]):
      Result[A] =
    rollbackOnException(savepointName)(a) flatMap { result =>
      commit map (_ => result)
    }
}

object ConnectionRef {
  def apply[S](connection: => Xor[SQL.Exceptions, Connection]):
      State[S, ConnectionRef[S]] =
    State.pure(new ConnectionRef[S](connection))

  def lift0[S, A, B](fn: Connection => A):
      ConnectionRef[S] => SQL.Result[S, A] = { cRef =>
    cRef.connection flatMap { c =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(c)))
    }
  }

  def lift1[S, A, B](fn: Connection => A => B):
      ConnectionRef[S] => SQL.Fun1[S, A, B] = { cRef => a =>
    cRef.connection flatMap { conn =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(conn)(a)))
    }
  }

  def lift2[S, A, B, C](fn: Connection => A => B => C):
      ConnectionRef[S] => SQL.Fun2[S, A, B, C] = { cRef => a => b =>
    cRef.connection flatMap { conn =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(conn)(a)(b)))
    }
  }
}
