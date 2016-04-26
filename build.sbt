name := "HelloSpark2"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"


libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.2.4" % "test",
	"org.apache.spark"  %% "spark-sql" % sparkVersion,
	"commons-io" % "commons-io" % "2.4",
	"joda-time" % "joda-time" % "2.9.3"
)

