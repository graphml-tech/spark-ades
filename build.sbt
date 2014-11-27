import AssemblyKeys._ // put this at the top of the file,leave the next line blank

name := "Spark ADES"

version := "0.0.1-SNAPSHOT"

organization := "com.spark.ades"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

scalacOptions += "-feature"

// Load Assembly Settings

assemblySettings

// Assembly App

mainClass in assembly := Some("com.spark.aers.ADES")

jarName in assembly := "sparkades-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

libraryDependencies ++= Seq(
  "org.apache.commons"  % "commons-math"    % "2.0",
  "org.apache.spark"  %% "spark-core"       % "1.1.0" % "provided"
  )

// Test Dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest"   % "2.2.0" % "test"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

