// -*- mode: scala -*-
//
// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
name := "H2Odb"

version := "0.2.0-SNAPSHOT"

organization := "org.truffulatree"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.14" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.healthmarketscience.jackcess" % "jackcess" % "1.2.13",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.scala-sbt" % "launcher-interface" % "0.12.0" % "provided")

resolvers ++= Seq(
  "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "https://oss.sonatype.org/content/repositories/releases")

resolvers <+= sbtResolver

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-explaintypes",
  "-Xlint")
