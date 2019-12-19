name := "DataFrames"

version := "0.1"

scalaVersion := "2.12.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  "org.apache.spark" %% "spark-mllib" %"2.4.0"

)
