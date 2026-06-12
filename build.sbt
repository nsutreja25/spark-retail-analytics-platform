name := "spark-retail-analytics-platform"

version := "1.0.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",

"org.apache.hadoop" % "hadoop-client" % "3.3.4",
"org.apache.hadoop" % "hadoop-common" % "3.3.4"
)
