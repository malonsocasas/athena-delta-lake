name := "athena-delta-lake"

version := "0.1"

scalaVersion := "2.12.13"

organization := "com.backmarket"

lazy val fetchAthenaSdk = taskKey[Unit]("Downloads AWS Athena Federation SDK jar in lib directory")

fetchAthenaSdk := {
  val log = streams.value.log
  import sys.process._
  val fileUrl = "https://github.com/awslabs/aws-athena-query-federation/releases/download/v2021.14.1/aws-athena-federation-sdk-2021.14.1.jar"
  val outputFilePath = (unmanagedBase in Compile).value.getPath + "/aws-athena-federation-sdk-2021.14.1.jar"
  log.info(s"Downloading athena SDK to $outputFilePath")
  new URL(fileUrl) #> new File(outputFilePath) !!
}


val filesToExclude = Seq(
  "linux/i386/libzstd-jni.so",
  "linux/mips64/libzstd-jni.so",
  "darwin/aarch64/libzstd-jni.dylib",
  "linux/amd64/libzstd-jni.so",
  "freebsd/i386/libzstd-jni.so",
  "linux/ppc64le/libzstd-jni.so",
  "linux/s390x/libzstd-jni.so",
  "linux/ppc64/libzstd-jni.so",
  "darwin/x86_64/libzstd-jni.dylib",
  "freebsd/amd64/libzstd-jni.so",
  "aix/ppc64/libzstd-jni.so"
)

lazy val root = project.in(file("."))
  .settings(libraryDependencies ++= Seq(
    "io.delta" %% "delta-standalone" % "0.2.0",
    "org.apache.hadoop" % "hadoop-client" % "3.3.0",
    "org.apache.hadoop" % "hadoop-aws" % "3.3.0",
    "org.apache.parquet" % "parquet-hadoop" % "1.12.0",
    "software.amazon.awssdk" % "s3" % "2.16.26",
    "com.amazonaws" % "aws-java-sdk-athena" % "1.11.563",
    "com.amazonaws" % "aws-lambda-java-core" % "1.2.0" % "provided",
    "com.amazonaws" % "aws-java-sdk-lambda" % "1.11.563" % "provided",
    "org.apache.arrow" % "arrow-vector" % "0.11.0",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.61"
  ))
  .settings(dependencyOverrides ++= {
    Seq(
      "com.github.mjakubowski84" %% "parquet4s-core" % "1.8.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"
    )
  })
  .settings(test in assembly := {})
  .settings(retrieveManaged := true)
  .settings(assemblyMergeStrategy in assembly := {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case "application.conf"                            => MergeStrategy.concat
    case "unwanted.txt"                                => MergeStrategy.discard
    case PathList("models",   xs @ _*)                 => MergeStrategy.discard
    case PathList("webapps",   xs @ _*)                => MergeStrategy.discard
    case PathList("win",   xs @ _*)                => MergeStrategy.discard
    case PathList("com", "amazonaws", "services", xs @ _*)         => {
      xs match {
        case "athena" :: _ =>  MergeStrategy.first
        case "lambda" :: _ =>  MergeStrategy.first
        case "s3" :: _ =>  MergeStrategy.first
        case "secretsmanager" :: _ =>  MergeStrategy.first
        case "dynamodbv2" :: _ =>  MergeStrategy.first
        case _ => MergeStrategy.discard
      }
    }
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.filterDistinctLines
      }
    case PathList(xs @ _*) if filesToExclude.contains(xs.mkString("/")) => MergeStrategy.discard
    case _                                             => MergeStrategy.first
  })