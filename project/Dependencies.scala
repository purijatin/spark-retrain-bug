import sbt._

object Dependencies {
  val sparkVersion = "2.3.0"

  val spark = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-streaming" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-sql" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-repl" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-mllib" % sparkVersion withSources()
  )

  val log = Seq(
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "org.slf4j" % "slf4j-log4j12" % "1.7.25"
  )

  val stemming = Seq(
    "org.apache.lucene" % "lucene-snowball" % "3.0.3"
  )

  val test = Seq(
    "org.scalacheck" %% "scalacheck" % "1.14.0"  % "test",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  )

}
