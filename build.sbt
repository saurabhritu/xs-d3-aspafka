name := "Aspafka"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-streaming" % "3.1.2",
//  "org.apache.spark" %% "spark-streaming-kafka" % "3.1.2",
  "org.apache.kafka" %% "kafka" % "3.0.0",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2",
  "org.apache.spark" % "spark-streaming_2.12" % "3.1.2" % "provided"

)
