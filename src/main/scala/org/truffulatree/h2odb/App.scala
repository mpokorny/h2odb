// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import scala.swing._
import scala.swing.event._
import javax.swing.UIManager
import javax.swing.filechooser.FileNameExtensionFilter
import java.awt.{Cursor, Dimension, Font}
import scala.concurrent.SyncVar
import java.io.{File, FileInputStream}
import com.healthmarketscience.jackcess.Database
import org.apache.poi.hssf.usermodel.HSSFWorkbook

object SwingApp extends SimpleSwingApplication {
  private val exitVal = new SyncVar[Int]()

  def appMain(args: Array[String]): xsbti.MainResult = {
    main(args)
    new Exit(exitVal.get)
  }

  override def quit() {
    shutdown()
    exitVal.put(0)
  }

  class SelectButton(
    val buttonText: String,
    val fileFilter: FileNameExtensionFilter,
    val field: TextField)
      extends Button {
    button =>

    text = buttonText

    var chooser: FileChooser = _

    resetChooser(None)

    def resetChooser(dir: Option[File]) {
      chooser = new FileChooser(dir.getOrElse(null)) {
        fileFilter = button.fileFilter
        multiSelectionEnabled = false
        fileSelectionMode = FileChooser.SelectionMode.FilesOnly
        title = buttonText
      }
    }
  }

  class FileSelectorPanel(
    tooltip: String,
    buttonText: String,
    fileFilter: FileNameExtensionFilter)
      extends BoxPanel(Orientation.Horizontal) {
    panel =>

    val textField = new TextField(20) {
      tooltip = panel.tooltip
      editable = false
    }

    val selectButton =
      new SelectButton(buttonText, fileFilter, textField)

    def reset() {
      if (!selectButton.field.text.isEmpty) {
        val file = new File(selectButton.field.text)
        if (file.isFile)
          selectButton.resetChooser(Some(new File(file.getParent)))
        selectButton.field.text = ""
      } else {
        selectButton.resetChooser(None)
      }
    }

    contents += selectButton
    contents += textField
    border = Swing.EmptyBorder(5)
  }

  val buttonPanel = new BoxPanel(Orientation.Horizontal) {
    val quitButton = new Button {
      text = "Quit"
    }
    val goButton = new Button {
      text = "Go"
      enabled = false
    }
    contents += goButton
    contents += quitButton
    border = Swing.EmptyBorder(10)
  }

  def top = new MainFrame {
    title = "H2Odb"
    val xlsPanel = new FileSelectorPanel(
      "Water analysis report (Excel file)",
      "Select report",
      new FileNameExtensionFilter("Excel file", "xls"))
    val dbPanel = new FileSelectorPanel(
      "Database (Access file)",
      "Select database",
      new FileNameExtensionFilter("Access database", "mdb"))
    contents = new BoxPanel(Orientation.Vertical) {
      contents += xlsPanel
      contents += dbPanel
      contents += buttonPanel
      border = Swing.EmptyBorder(30, 30, 10, 30)
    }
    listenTo(buttonPanel.quitButton)
    listenTo(buttonPanel.goButton)
    listenTo(xlsPanel.selectButton)
    listenTo(dbPanel.selectButton)
    listenTo(xlsPanel.selectButton.field)
    listenTo(dbPanel.selectButton.field)
    reactions += {
      case ButtonClicked(b) if b == buttonPanel.quitButton =>
        quit()
      case ButtonClicked(b) if b == buttonPanel.goButton => {
        buttonPanel.goButton.enabled = false
        val xlsPath = xlsPanel.selectButton.field.text
        val dbPath = dbPanel.selectButton.field.text
        val origCursor = cursor
        cursor = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR)
        runFiller(xlsPath, dbPath)
        xlsPanel.reset()
        dbPanel.reset()
        cursor = origCursor
      }
      case ButtonClicked(b: SelectButton) => {
        b.chooser.showDialog(b, "Select") match {
          case FileChooser.Result.Approve =>
            b.field.text = b.chooser.selectedFile.getPath
          case _ =>
        }
      }
      case ValueChanged(f: TextField)
          if (f == xlsPanel.selectButton.field ||
            f == dbPanel.selectButton.field) => {
            if (!(xlsPanel.selectButton.field.text.isEmpty ||
              dbPanel.selectButton.field.text.isEmpty))
              buttonPanel.goButton.enabled = true
            else
              buttonPanel.goButton.enabled = false
          }
    }
  }

  def runFiller(xlsPath: String, dbPath: String) {

    class OpenException(m: String) extends Exception(m)

    def openFile[A](path: String, opener: (String) => A, desc: String): A =
      try {
        opener(path)
      } catch {
        case _: java.io.IOException =>
          throw new OpenException(s"Failed to open ${path} as ${desc}")
      }
    try {
      val xls = openFile(
        xlsPath,
        (s: String) => new HSSFWorkbook(new FileInputStream(s)),
        "an Excel file")
      val db =
        openFile(
          dbPath,
          (s: String) => Database.open(new File(s), false, false),
          "an Access database")
      val resultsFrame = new Frame {
        val textArea = new TextArea(10, 40) {
          lineWrap = true
          wordWrap = true
        }
        contents = new BoxPanel(Orientation.Vertical) {
          val scrollPane = new ScrollPane {
            contents = textArea
            verticalScrollBarPolicy = ScrollPane.BarPolicy.AsNeeded
            horizontalScrollBarPolicy = ScrollPane.BarPolicy.Never
          }
          val desc = new TextField {
            editable = false
            text = s"${(new File(xlsPath)).getName} loaded into ${(new File(dbPath)).getName}"
            font = font.deriveFont(Font.BOLD)
            horizontalAlignment = Alignment.Center
            border = Swing.EmptyBorder(10, 0, 10, 0)
          }
          contents += desc
          contents += scrollPane
        }
        title = "Results"
      }
      try {
        DBFiller(resultsFrame.textArea, xls, db)
      } catch {
        case dbe: H2ODbException =>
          resultsFrame.textArea.append(dbe.getMessage)
      }
      if (resultsFrame.size == new Dimension(0, 0)) resultsFrame.pack()
      resultsFrame.visible = true
    } catch {
      case oe: OpenException =>
        Dialog.showOptions(
          message = oe.getMessage,
          title = "File Error",
          optionType = Dialog.Options.Default,
          messageType = Dialog.Message.Error,
          entries = List("OK"),
          initial = 0)
    }
  }
}

class Exit(val code: Int) extends xsbti.Exit

class Run extends xsbti.AppMain {
  def run(configuration: xsbti.AppConfiguration): xsbti.MainResult = {
    configuration.provider.scalaProvider.version match {
      case "2.10.2" => {
        try {
          SwingApp.appMain(configuration.arguments)
        } catch {
          case e: Exception => {
            println(e)
            new Exit(1)
          }
        }
      }
      case _ => new xsbti.Reboot {
        def arguments = configuration.arguments
        def baseDirectory = configuration.baseDirectory
        def scalaVersion = "2.10.2"
        def app = configuration.provider.id
      }
    }
  }
}
