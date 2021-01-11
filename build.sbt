name := "recommend-engine"
version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

val akkaHttpVersion = "10.1.7"
val akkaVersion = "2.5.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql"   % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream"          % akkaVersion
)

