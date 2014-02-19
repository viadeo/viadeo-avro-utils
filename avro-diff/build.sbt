
name := "avro-diff"

version := "1.0"

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.6",
  "org.apache.avro" % "avro-mapred" % "1.7.6",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.1.2",
  "org.apache.mrunit" % "mrunit" % "1.0.0" % "test" classifier "hadoop2"
)

