name := "apache-spark-scala"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Typesafe repository releases" at "http://repo.typesafe.com/typesafe/releases/"
//provided ignores jars that are already part of the container like Spark https://github.com/sbt/sbt-assembly#excluding-jars-and-files
libraryDependencies ++= Seq (
  "org.scalatest" %% "scalatest" % "2.2.6" % Test,
  "org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.6.3"
)

mainClass in assembly := Some("SparkPageRank")

/*
assemblySettings

jarName in assembly := "apache-spark-scala-asembly.jar"

assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)*/
