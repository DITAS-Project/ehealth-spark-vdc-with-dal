
name := "vdc-dal-grpc"

organization := "com.ditas"


version := "0.1"

scalaVersion := "2.11.7"

resolvers += Resolver.mavenLocal


PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-aws" % "2.8.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.3",
  "joda-time" % "joda-time" % "2.9.9",
  "com.amazonaws" % "aws-java-sdk-core" % "1.11.234",
  "com.amazonaws" % "aws-java-sdk" % "1.11.234",
  "com.amazonaws" % "aws-java-sdk-kms" % "1.11.234",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.234",
  "org.apache.hadoop" % "hadoop-client" % "2.8.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.0",
  "org.apache.spark" % "spark-core_2.11" % "2.3.0-GG" exclude("org.apache.hadoop","hadoop-client"),
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0-GG",
  "org.yaml" % "snakeyaml" % "1.11",
  "com.typesafe.play" %% "play-json" % "2.6.6",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "mysql" % "mysql-connector-java" % "6.0.6",
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
)

libraryDependencies ~= { _.map(_.exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.11")) }

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.2"

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
libraryDependencies ~= { _.map(_.exclude("log4j", "log4j")) }

dependencyOverrides ++= {
  Seq(
    "org.apache.spark" % "spark-core_2.11" % "2.3.0-GG",
    "org.apache.spark" % "spark-sql_2.11" % "2.3.0-GG"
  )
}

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

