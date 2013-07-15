package org.truffulatree.h2odb

import java.io.{File, FileReader}
import au.com.bytecode.opencsv.CSVReader
import com.healthmarketscience.jackcess.Database

object Main extends App {

  def checkArgs(args: String*): (Option[CSVReader], Option[Database]) = {
    if (args.length != 2) (None, None)
    else (
      try {
        Some(new CSVReader(new FileReader(new File(args(0))), ';'))
      } catch {
        case _: Exception => {
          println(s"Failed to open ${args(0)} as an CSV file")
          None
        }
      },
      try {
        Some(Database.open(new File(args(1)), false, false))
      } catch {
        case _: Exception => {
          println(s"Failed to open ${args(1)} as an Access file")
          None
        }
      })
  }

  checkArgs(args:_*) match {
    case (Some(csv), Some(db)) => {
      try {
        DBFiller(csv, db)
      } catch {
        case e: H2ODbException => println(e.getMessage)
      } finally {
        db.close()
      }
    }
    case _ =>
  }
}
