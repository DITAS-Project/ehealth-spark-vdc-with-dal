import sbt.ExclusionRule

name := "ehealth-dal"

organization := "com.ditas"


version := "0.1"

scalaVersion := "2.11.7"

resolvers += Resolver.mavenLocal


libraryDependencies ++= {
  val sparkVersion = "2.3.0"
  val hadoopVersion = "2.8.2"
  Seq(
    "org.apache.hadoop" % "hadoop-aws" % "2.8.2",
    "org.apache.httpcomponents" % "httpclient" % "4.5.3",
    "joda-time" % "joda-time" % "2.9.9",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.8.0",

    "org.apache.spark" % "spark-core_2.11" % sparkVersion excludeAll(
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-hadoop"),
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-format"),
      ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-client")),
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion excludeAll(
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-hadoop"),
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-format")),
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-hadoop"),
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-format")),
    "org.apache.hadoop" % "hadoop-aws" % hadoopVersion excludeAll(
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-hadoop"),
      ExclusionRule(organization = "org.apache.parquet", name = "parquet-format")),
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.234",

    "org.yaml" % "snakeyaml" % "1.11",
    "com.typesafe.play" %% "play-json" % "2.6.6",
    "org.scalaj" %% "scalaj-http" % "2.4.1",
    "org.slf4j" % "slf4j-api" % "1.7.25",
    "mysql" % "mysql-connector-java" % "6.0.6",
    "org.apache.httpcomponents" % "httpcore" % "4.4.8",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.auth0" % "jwks-rsa" % "0.8.0",
    "com.auth0" % "java-jwt" % "3.8.0",
    "com.pauldijou" %% "jwt-core" % "2.1.0",
    "junit" % "junit" % "4.12" % Test,
    "io.grpc" % "grpc-testing" % "1.15.1" % Test
  )
}
libraryDependencies ~= { _.map(_.exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.11")) }
libraryDependencies ~= { _.map(_.exclude("com.fasterxml.jackson.core", "jackson-databind")) }
libraryDependencies ~= { _.map(_.exclude("com.google.guava", "guava")) }

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"



enablePlugins(JavaAppPackaging)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.contains("services") => MergeStrategy.concat
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

