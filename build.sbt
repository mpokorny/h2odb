// -*- mode: scala -*-
//
// Copyright 2013, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
name := "H2Odb"

version := "0.6.1"

organization := "org.truffulatree"

licenses := Seq(
  "Mozilla Public License Version 2.0" -> url("https://mozilla.org/MPL/2.0/"))

homepage := Some(url("https://github.com/mpokorny/h2odb"))

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" % "scala-swing_2.11" % "1.0.1",
  "com.healthmarketscience.jackcess" % "jackcess" % "2.0.4",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "org.apache.poi" % "poi" % "3.9",
  "org.scala-sbt" % "launcher-interface" % "0.13.5" % "provided")

resolvers ++= Seq(
  "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases"  at "https://oss.sonatype.org/content/repositories/releases")

resolvers <+= sbtResolver

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-explaintypes",
  "-Xlint")

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("staging"  at nexus + "service/local/staging/deploy/maven2")
}

credentials += Credentials(Path.userHome / ".sbt" / "sonatype.txt")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:mpokorny/h2odb.git</url>
    <connection>scm:git:git@github.com:mpokorny/h2odb.git</connection>
  </scm>
  <developers>
    <developer>
      <id>martin</id>
      <name>Martin Pokorny</name>
      <email>martin@truffulatree.org</email>
      <timezone>America/Denver</timezone>
    </developer>
  </developers>)

useGpg := true
