name := "spark-prac"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Dependencies.spark ++ Seq(
  "com.sparkjava" % "spark-core" % "2.7.2"
).map(_.exclude("javax.servlet", "servlet-api")) ++ Dependencies.log ++
  Dependencies.stemming ++ Dependencies.test
