name := "SparkDataPipeline"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "com.databricks" % "spark-redshift_2.10" % sparkVersion,
  "com.typesafe" % "config" % "1.3.1"
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"
// https://mvnrepository.com/artifact/com.databricks/spark-redshift_2.10
//libraryDependencies += "com.databricks" % "spark-redshift_2.10" % "2.0.1"




resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Apache Spark Core" at "https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.10",
  "Apache Spark SQL" at "https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10",
  "AWS Redshift Driver" at "https://mvnrepository.com/artifact/com.databricks/spark-redshift_2.10",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}