// -*- mode: scala -*-
//
// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
name := "H2Odb"

version := "0.1.0"

organization := "org.truffulatree"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.specs2" %% "specs2" % "1.14" % "test",
  "org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.healthmarketscience.jackcess" % "jackcess" % "1.2.13",
  "org.apache.logging.log4j" % "log4j-api" % "2.0-beta8",
  "org.apache.logging.log4j" % "log4j-core" % "2.0-beta8")

resolvers ++= Seq(
  "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "https://oss.sonatype.org/content/repositories/releases")

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-feature",
  "-explaintypes",
  "-Xlint")
