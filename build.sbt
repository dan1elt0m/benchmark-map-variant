ThisBuild / scalaVersion     := "2.13.14"
ThisBuild / version          := "0.1.0-SNAPSHOT"

ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq(
"--add-exports",
"java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
.settings(
  name := "SparkBenchmark",
  libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "4.0.0-preview1",
     "org.apache.spark" %% "spark-sql" % "4.0.0-preview1",
     "org.knowm.xchart" % "xchart" % "3.8.0",
      "io.delta" %% "delta-spark" % "4.0.0rc1"
  ),
  dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-5"
)