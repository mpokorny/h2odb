// -*- mode: scala -*-
//
// Copyright 2013,2016, Martin Pokorny <martin@truffulatree.org>
//
// This Source Code Form is subject to the terms of the Mozilla Public License,
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.
//
name := "H2Odb"

version := "0.12.1-SNAPSHOT"

organization := "org.truffulatree"

licenses := Seq(
    "Mozilla Public License Version 2.0" -> url("https://mozilla.org/MPL/2.0/"))

homepage := Some(url("https://github.com/mpokorny/h2odb"))

scalaVersion := "2.11.8"

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")

libraryDependencies ++= Seq(
    "com.healthmarketscience.jackcess" % "jackcess" % "2.0.4",
    "com.typesafe.play" %% "anorm" % "2.5.2",
    jdbc,
    "org.apache.poi" % "poi" % "3.14",
    "org.scala-lang.modules" %% "scala-swing" % "2.0.0-M2",
    "org.typelevel" %% "cats" % "0.6.0",
    "org.scalacheck" %% "scalacheck" % "1.13.1" % "test",
    "org.scalatest" %% "scalatest" % "3.0.0" % "test")

resolvers ++= Seq(
    "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "releases"  at "https://oss.sonatype.org/content/repositories/releases")

resolvers <+= sbtResolver

scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-explaintypes",
    "-Xlint")

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "commons", "logging", _*) =>
    MergeStrategy.first

  case x@_ =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value

    oldStrategy(x)
}

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
