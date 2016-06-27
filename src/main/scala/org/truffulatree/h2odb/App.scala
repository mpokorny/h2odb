// Copyright 2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
package org.truffulatree.h2odb

import java.awt.{Cursor, Dimension, Font}
import java.awt.datatransfer.StringSelection
import java.io.{File, FileInputStream}
import java.sql.SQLException

import scala.concurrent.SyncVar
import scala.swing._
import scala.swing.event._

import com.microsoft.sqlserver.jdbc.SQLServerDataSource
import javax.swing.JPopupMenu
import javax.swing.filechooser.FileNameExtensionFilter
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.slf4j.LoggerFactory

/** Add xsbti.MainResult return value to SimpleSwingApplication.main().
  */
trait SwingAppMain {
  self: SimpleSwingApplication =>

  private val exitVal = new SyncVar[Int]()

  /** Call SimpleSwingApplication.main(), then wait until exitVal has a value.
    */
  def apply(args: Array[String]): xsbti.MainResult = {
    main(args)

    new Exit(exitVal.take())
  }

  override def quit(): Unit = {
    shutdown()

    exitVal.put(0)
  }
}

object PopupMenu {
  private[PopupMenu] trait JPopupMenuMixin { def popupMenuWrapper: PopupMenu }

  def defaultLightWeightPopupEnabled: Boolean =
    JPopupMenu.getDefaultLightWeightPopupEnabled

  def defaultLightWeightPopupEnabled_=(aFlag: Boolean): Unit = {
      JPopupMenu.setDefaultLightWeightPopupEnabled(aFlag)
    }
}

class PopupMenu extends Component with SequentialContainer.Wrapper {

  override lazy val peer: JPopupMenu =
    new JPopupMenu with PopupMenu.JPopupMenuMixin with SuperMixin {
      def popupMenuWrapper = PopupMenu.this
    }

  def lightWeightPopupEnabled: Boolean = peer.isLightWeightPopupEnabled

  def lightWeightPopupEnabled_=(aFlag: Boolean): Unit = {
      peer.setLightWeightPopupEnabled(aFlag)
    }

  def show(invoker: Component, x: Int, y: Int): Unit = {
    peer.show(invoker.peer, x, y)
  }
}

object SwingApp extends SimpleSwingApplication with SwingAppMain {

  class SelectButton(
    val buttonText: String,
    val fileFilter: FileNameExtensionFilter,
    val field: TextField)
      extends Button {
    button =>

    text = buttonText

    var chooser: FileChooser = _

    resetChooser(None)

    def resetChooser(dir: Option[File]): Unit = {
      chooser = new FileChooser(dir.getOrElse(null)) {
          fileFilter = button.fileFilter
            multiSelectionEnabled = false
            fileSelectionMode = FileChooser.SelectionMode.FilesOnly
            title = button.text
        }
    }
  }

  class FileSelectorPanel(
    tooltip: String,
    buttonText: String,
    fileFilter: FileNameExtensionFilter)
      extends BoxPanel(Orientation.Horizontal) {
    panel =>

    val textFieldWidth = 20

    val textField = new TextField(textFieldWidth) {
        tooltip = panel.tooltip

        editable = false
      }

    val selectButton = new SelectButton(buttonText, fileFilter, textField)

    def reset(): Unit = {
      if (!selectButton.field.text.isEmpty) {
        val file = new File(selectButton.field.text)
        if (file.isFile) {
          selectButton.resetChooser(Some(new File(file.getParent)))
        }

        selectButton.field.text = ""
      } else {
        selectButton.resetChooser(None)
      }
    }

    contents += selectButton

    contents += textField

    val borderWidth = 5

    border = Swing.EmptyBorder(borderWidth)
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

      val borderWidth = 10

      border = Swing.EmptyBorder(borderWidth)
    }

  def top = new MainFrame {
      title = "H2Odb"

      val xlsPanel = new FileSelectorPanel(
          "Water analysis report (Excel file)",
          "Select report",
          new FileNameExtensionFilter("Excel file", "xls"))

      val dbConnectionParameters = new GridPanel(5, 2) {
          def inputField(name: String, fieldConstructor: Int => TextField):
              TextField = {
            val fieldWidth = 20

            val label = new Label(name)

            label.horizontalAlignment = Alignment.Right

            contents += label

            val field = fieldConstructor(fieldWidth)

            contents += field

            field
          }

          val usernameField = inputField("Username", new TextField(_))

          val passwordField = inputField("Password", new PasswordField(_))

          val serverNameField = inputField("Server hostname",  new TextField(_))

          val portField = inputField("Port", new TextField(_))

          val databaseNameField = inputField("Database name",  new TextField(_))
        }

      contents = new BoxPanel(Orientation.Vertical) {
          contents += xlsPanel

          contents += dbConnectionParameters

          border = Swing.EmptyBorder(30, 30, 10, 30)
        }

      reactions += {
        case ButtonClicked(b) if b == buttonPanel.quitButton =>
          quit()

        case ButtonClicked(b) if b == buttonPanel.goButton => {
            buttonPanel.goButton.enabled = false

            val xlsPath = xlsPanel.selectButton.field.text

            val username = dbConnectionParameters.usernameField.text

            val password = dbConnectionParameters.passwordField.text

            val serverName = dbConnectionParameters.serverNameField.text

            val port = dbConnectionParameters.portField.text.toInt

            val databaseName = dbConnectionParameters.databaseNameField.text

            val origCursor = cursor

            cursor = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR)

            runFiller(xlsPath, username, password, serverName, port, databaseName)

            xlsPanel.reset()

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
                  f == dbConnectionParameters.usernameField ||
                  f == dbConnectionParameters.passwordField ||
                  f == dbConnectionParameters.serverNameField ||
                  f == dbConnectionParameters.portField ||
                  f == dbConnectionParameters.databaseNameField) => {
              if (xlsPanel.selectButton.field.text.isEmpty ||
                    dbConnectionParameters.usernameField.text.isEmpty ||
                    dbConnectionParameters.passwordField.text.isEmpty ||
                    dbConnectionParameters.serverNameField.text.isEmpty ||
                    dbConnectionParameters.portField.text.isEmpty ||
                    dbConnectionParameters.databaseNameField.text.isEmpty) {
                buttonPanel.goButton.enabled = false
              } else {
                buttonPanel.goButton.enabled = true
              }
            }
      }

      listenTo(buttonPanel.quitButton)

      listenTo(buttonPanel.goButton)

      listenTo(xlsPanel.selectButton)

      listenTo(xlsPanel.selectButton.field)

      listenTo(dbConnectionParameters.usernameField)

      listenTo(dbConnectionParameters.passwordField)

      listenTo(dbConnectionParameters.serverNameField)

      listenTo(dbConnectionParameters.portField)

      listenTo(dbConnectionParameters.databaseNameField)
    }

  def runFiller(
    xlsPath: String,
    username: String,
    password: String,
    serverName: String,
    port: Int,
    databaseName: String): Unit = {

    class OpenException(m: String) extends Exception(m)

    def openFile[A](path: String, opener: (String) => A, desc: String): A =
      try {
        opener(path)
      } catch {
        case _: java.io.IOException =>
          throw new OpenException(s"Failed to open ${path} as ${desc}")
      }

    try {
      val (xlsFile, xls) = openFile(
          xlsPath,
          (s: String) => {
            val fis = new FileInputStream(s)

            (fis, new HSSFWorkbook(fis))
          },
          "an Excel file")

      val ds = new SQLServerDataSource

      ds.setUser(username)

      ds.setPassword(password)

      ds.setServerName(serverName)

      ds.setPortNumber(port)

      ds.setDatabaseName(databaseName)

      val resultsFrame = new Frame {
          title = "Results"

          val textArea = new TextArea(10, 40) {
              textArea =>

              lineWrap = true
                wordWrap = true

              reactions += {
                case ev @ MousePressed(_,_,_,_,true) => showPopupMenu(ev)
                case ev @ MouseReleased(_,_,_,_,true) => showPopupMenu(ev)
              }

              listenTo(mouse.clicks)

              def showPopupMenu(event: MouseEvent): Unit = {
                if (selected != null && selected.length > 0)
                  popupMenu.show(event.source, event.point.x, event.point.y)
              }

              val popupMenu = new PopupMenu {
                  contents += new MenuItem(
                      Action("Copy") {
                        val selection = new StringSelection(textArea.selected)

                        toolkit.getSystemClipboard.setContents(selection, selection)

                        this.visible = false
                      })
                }
            }

          contents = new BoxPanel(Orientation.Vertical) {

              contents += new TextField {
                  editable = false

                  text = s"${(new File(xlsPath)).getName} loaded into $databaseName"

                  font = font.deriveFont(Font.BOLD)

                  horizontalAlignment = Alignment.Center

                  border = Swing.EmptyBorder(10)

                  maximumSize = preferredSize
                }

              contents += new ScrollPane {
                  contents = textArea

                  verticalScrollBarPolicy = ScrollPane.BarPolicy.AsNeeded

                  horizontalScrollBarPolicy = ScrollPane.BarPolicy.Never
                }
            }
        }

      val connection = ds.getConnection

      connection.setAutoCommit(true)

      val filler = new sql.DBFiller()(connection)

      filler.getFromWorkbook(
        (s: String) => resultsFrame.textArea.append(s + "\n"),
        xls)

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

      case se: SQLException =>
        Dialog.showOptions(
          message = se.getMessage,
          title = "Database Error",
          optionType = Dialog.Options.Default,
          messageType = Dialog.Message.Error,
          entries = List("OK"),
          initial = 0)
    }
  }
}

class Exit(val code: Int) extends xsbti.Exit

class Run extends xsbti.AppMain {
  private val logger = LoggerFactory.getLogger(getClass.getName.init)
  def run(configuration: xsbti.AppConfiguration): xsbti.MainResult = {
    configuration.provider.scalaProvider.version match {
      case "2.11.8" => {
        try {
          SwingApp(configuration.arguments)
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
            new Exit(1)
          }
        }
      }
      case _ => new xsbti.Reboot {
        def arguments = configuration.arguments
        def baseDirectory = configuration.baseDirectory
        def scalaVersion = "2.11.8"
        def app = configuration.provider.id
      }
    }
  }
}
