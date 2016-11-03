name := "Practice 001"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "en-japan Maven OSS" at "http://dl.bintray.com/en-japan/maven-oss"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided",
 "com.atilika.kuromoji" % "kuromoji-ipadic" % "0.9.0"
)

