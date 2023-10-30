ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.2"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.0.2" % "provided"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.0.2"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.2"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "micro_batch_processor"
  )
