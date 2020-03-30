name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion: String = "3.0.0-preview"
val vegasVersion: String = "0.3.11"
val postgreVersion: String = "42.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  "org.postgresql" % "postgresql" % postgreVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
)