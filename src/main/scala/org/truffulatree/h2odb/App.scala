// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import java.io.{File, FileInputStream}
import com.healthmarketscience.jackcess.Database
import org.apache.poi.hssf.usermodel.HSSFWorkbook

object Main {
  def checkArgs(args: String*): (Option[HSSFWorkbook], Option[Database]) = {
    if (args.length != 2) {
      println("Usage: dbfiller [XLS file] [MDB file]")
      (None, None)
    }
    else (
      try {
        Some(new HSSFWorkbook(new FileInputStream(args(0))))
      } catch {
        case e: Exception => {
          println(e)
          println(s"Failed to open ${args(0)} as an XLS file")
          None
        }
      },
      try {
        Some(Database.open(new File(args(1)), false, false))
      } catch {
        case e: Exception => {
          println(e)
          println(s"Failed to open ${args(1)} as an Access file")
          None
        }
      })
  }

  def apply(args: String*): xsbti.MainResult = {
    checkArgs(args:_*) match {
      case (Some(xls), Some(db)) => {
        try {
          DBFiller(xls, db)
          new Exit(0)
        } catch {
          case e: H2ODbException => {
            println(e.getMessage)
            new Exit(1)
          }
        } finally {
          db.close()
        }
      }
      case _ => new Exit(1)
    }
  }
}

class Exit(val code: Int) extends xsbti.Exit

class Run extends xsbti.AppMain {
  def run(configuration: xsbti.AppConfiguration): xsbti.MainResult = {
    configuration.provider.scalaProvider.version match {
      case "2.10.2" => Main(configuration.arguments:_*)
      case _ => new xsbti.Reboot {
        def arguments = configuration.arguments
        def baseDirectory = configuration.baseDirectory
        def scalaVersion = "2.10.2"
        def app = configuration.provider.id
      }
    }
  }
}
