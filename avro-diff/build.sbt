
name := "avro-diff"

version := "1.0"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.6",
  "org.apache.avro" % "avro-mapred" % "1.7.6",
  "org.apache.avro" % "avro-ipc" % "1.7.6",
  "org.apache.avro" % "avro-tools" % "1.7.6",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.1.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "test",
  "commons-httpclient" % "commons-httpclient" % "3.1" % "test"
)

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"

testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")

crossPaths := false

javacOptions ++= Seq("-source", "1.6")
