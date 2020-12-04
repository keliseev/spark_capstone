name := "GridUCapstone"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "2.4.7"),
  ("org.apache.spark" %% "spark-sql" % "2.4.7")
)
