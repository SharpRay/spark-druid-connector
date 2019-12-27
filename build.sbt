
val sparkVersion = "2.3.0"
val json4sVersion = "3.6.0-M2"
val jodaVersion = "2.9.9"
val curatorVersion = "4.0.1"
val jacksonVersion = "2.6.5"
val apacheHttpVersion = "4.5.5"

val myDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "joda-time" % "joda-time" % jodaVersion,
  "org.apache.curator" % "curator-framework" % curatorVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion,
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-smile-provider" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % apacheHttpVersion
)

lazy val commonSettings = Seq(
  organization := "org.rzlabs",
  version := "0.1.0-SNAPSHOT",
  
  scalaVersion := "2.11.8"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "spark-druid-connector",
    libraryDependencies ++= myDependencies
  )
