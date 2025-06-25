ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql"  % "3.5.6",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4"
)

fork := true

lazy val root = (project in file("."))
  .settings(
    name := "projet_spark",
    idePackagePrefix := Some("org.esgi")
  )
