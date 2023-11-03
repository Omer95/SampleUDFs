ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "SampleUDFs"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
