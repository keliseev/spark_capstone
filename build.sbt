name := "GridUCapstone"

version := "1.0"

scalaVersion := "2.12.12"

mainClass in Compile := Some("capstone.DemoApp")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "com.typesafe" % "config" % "1.3.3",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test)
