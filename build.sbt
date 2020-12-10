name := "GridUCapstone"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "2.4.7"),
  ("org.apache.spark" %% "spark-sql" % "2.4.7"),
  "com.typesafe" % "config" % "1.3.3"

  //  Examles scala test libriries
  //    ("org.scalatest" %% "scalatest" % "3.2.2" % Test),
  //  ("org.mockito" % "mockito-scala-scalatest_2.12" % "1.16.0"),
)
