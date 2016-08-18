// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb.sql

import java.sql.{Connection, Savepoint}
import javax.sql.DataSource

import scala.concurrent.duration.Duration

import cats.Show
import cats.data.{NonEmptyList, State, XorT}
import cats.std.list._
import cats.syntax.all._
import play.api.Logger
import org.jdbcdslog.ConnectionLoggingProxy

final class ConnectionRef[S, E] protected
  (dataSource: DataSource)(implicit EC: SQL.ErrorContext[E], ES: Show[E]) {
  self =>

  val E = implicitly[SQL.ErrorContext[E]]

  type ST[A] = State[S, A]

  type Result[A] = SQL.Result[S, E, A]

  type Fun1[A, B] = SQL.Fun1[S, E, A, B]

  private[this] val logger = Logger(getClass.getName.init)

  val connection: Result[Connection] =
    XorT.fromXor[ST](
      SQL.catchEx(ConnectionLoggingProxy.wrap(dataSource.getConnection)))

  private[this] def apply0[A](conn: Connection => A) =
    ConnectionRef.lift0(conn)(E)(self)

  private[this] def apply1[A, B](conn: Connection => A => B) =
    ConnectionRef.lift1(conn)(E)(self)

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
      XorT.right[ST, NonEmptyList[E], Option[Savepoint]](
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
      sp => XorT.right[ST, NonEmptyList[E], SavepointRef[S]](SavepointRef(sp))

    optName => setSp(optName) flatMap spRef
  }

  def releaseSavepoint: Fun1[SavepointRef[S], Unit] = { spr =>
    XorT.right[ST, NonEmptyList[E], Savepoint](spr.savepoint).
      flatMap(apply1(c => (sp: Savepoint) => c.releaseSavepoint(sp)))
  }

  def rollbackOnException[A](savepointName: Option[String])(a: Result[A]):
      Result[A] = {
    setSavepoint(savepointName) flatMap { spr =>
      val result =
        a.swap.flatMap(
          ex => XorT.right[ST, A, NonEmptyList[E]](
            rollback(Some(spr)).
              fold(ex1 => ex combine ex1, _ => ex))).swap

      releaseSavepoint(spr).recover {
        case exs@_ => logger.warn(
          s"""|Ignored error upon releasing database connection savepoint:
              | ${exs.unwrap.map(_.show).mkString("; ")}""".
            stripMargin)
      } flatMap (_ => result)
    }
  }

  def commitOnSuccess[A](savepointName: Option[String])(a: Result[A]):
      Result[A] =
    rollbackOnException(savepointName)(a) flatMap { result =>
      commit map (_ => result)
    }

  def closeOnCompletion[A](a: Result[A]): Result[A] =
    XorT[ST, NonEmptyList[E], A](
      a.value.flatMap { xa =>
        val cl =
          close.recover {
            case exs@_ => logger.warn(
              s"""|Ignored error upon closing database connection:
                  | ${exs.unwrap.map(_.show).mkString("; ")}""".
                stripMargin)
          }

        cl.value map (_ => xa)
      })
}

object ConnectionRef {
  def apply[S, A](
    dataSource: DataSource)(
    implicit EA: SQL.ErrorContext[A],
    SA: Show[A]):
      SQL.Result[S, A, ConnectionRef[S, A]] =
    XorT.right[State[S, ?], NonEmptyList[A], ConnectionRef[S, A]](
      State.pure(new ConnectionRef[S, A](dataSource)))

  def lift0[S, A : SQL.ErrorContext, B](fn: Connection => B):
      ConnectionRef[S, A] => SQL.Result[S, A, B] = { cRef =>
    cRef.connection flatMap { c =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(c)))
    }
  }

  def lift1[S, A : SQL.ErrorContext, B, C](fn: Connection => B => C):
      ConnectionRef[S, A] => SQL.Fun1[S, A, B, C] = { cRef => b =>
    cRef.connection flatMap { conn =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(conn)(b)))
    }
  }

  def lift2[S, A : SQL.ErrorContext, B, C, D](fn: Connection => B => C => D):
      ConnectionRef[S, A] => SQL.Fun2[S, A, B, C, D] = { cRef => b => c =>
    cRef.connection flatMap { conn =>
      XorT.fromXor[State[S, ?]](SQL.catchEx(fn(conn)(b)(c)))
    }
  }
}
