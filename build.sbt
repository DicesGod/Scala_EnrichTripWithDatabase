name := "course4project"

version := "0.1"

scalaVersion := "2.12.0"

val hadoopVersion = "2.7.3"

val btversion = "3.9.0"
/*
"organization" % "artifact" % "version"
 */

libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"
libraryDependencies += "com.github.pathikrit" %% "better-files" % btversion
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"

/*libraryDependencies ++= Seq (
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-hdfs",
).map( _ % hadoopVersion)*/
