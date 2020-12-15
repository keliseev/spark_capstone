name := "GridUCapstone"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "com.typesafe" % "config" % "1.3.3",
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test,
  "org.mockito" % "mockito-core" % "1.9.5" % Test)
