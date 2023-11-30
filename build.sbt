ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "NovaQualityTraining",
    idePackagePrefix := Some("es.novaquality.spark"),
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.2",
    libraryDependencies += "org.postgresql" % "postgresql" % "42.5.4",
    libraryDependencies += "com.github.pjfanning" %% "scala-faker" % "0.5.3",
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2"
  )
