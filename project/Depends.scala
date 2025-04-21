import sbt._
import sbt.{ ExclusionRule, ModuleID }

object Depends {
  // The following implicits will enable the scoping of seq of dependencies rather than single dependency
  implicit class implicits(sq: Seq[ModuleID]) {
    def as(str: String): Seq[ModuleID] =
      sq.map(_ % str)

    def exclude(org: String, name: String): Seq[ModuleID] =
      sq.map(_.exclude(org, name))

    def excludeAll(rules: ExclusionRule*): Seq[ModuleID] =
      sq.map(_.excludeAll(rules: _*))
  }

  lazy val spark: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-sql" % Versions.spark
  )

  lazy val sparkTests: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-sql"  % Versions.spark classifier "tests",
    "org.apache.spark" %% "spark-core" % Versions.spark classifier "tests"
  )

  lazy val delta: Seq[ModuleID] = Seq(
    "io.delta" %% "delta-spark" % Versions.delta
  )

  lazy val scalaLogging: Seq[ModuleID] = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  )

  lazy val awsSecretManager: Seq[ModuleID] = Seq(
    "software.amazon.awssdk" % "secretsmanager" % Versions.awsSecretManager
  )

  lazy val pureconfig: Seq[ModuleID] = Seq(
    "com.github.pureconfig" %% "pureconfig" % Versions.pureconfig
  )

  lazy val scopt: Seq[ModuleID] = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  lazy val scalatest: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest
  )

  lazy val monocle: Seq[ModuleID] = Seq(
    "com.github.julien-truffaut" %% "monocle-core"  % Versions.monocle,
    "com.github.julien-truffaut" %% "monocle-macro" % Versions.monocle
  )

  lazy val betterfiles: Seq[ModuleID] = Seq(
    "com.github.pathikrit" %% "better-files" % Versions.betterfiles
  )

  lazy val json4s: Seq[ModuleID] = Seq(
    "org.json4s" %% "json4s-core"    % Versions.json4s,
    "org.json4s" %% "json4s-jackson" % Versions.json4s
  )

  lazy val shapeless: Seq[ModuleID] = Seq(
    "com.chuusai" %% "shapeless" % Versions.shapeless
  )

  lazy val h2Database: Seq[ModuleID] = Seq(
    "com.h2database" % "h2" % Versions.h2
  )

  lazy val postgreSql: Seq[ModuleID] = Seq(
    "org.postgresql" % "postgresql" % Versions.postgresql
  )

  lazy val jackson = Seq(
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson
  )

  lazy val awsJavaSdkCore = Seq(
    "com.amazonaws" % "aws-java-sdk-core" % Versions.awsJavaSdkCore
  )

  lazy val awsJavaSdkSecretManager = Seq(
    "com.amazonaws" % "aws-java-sdk-secretsmanager" % Versions.awsJavaSdkSecretManager
  )

  lazy val awsJavaSdkS3 = Seq(
    "com.amazonaws" % "aws-java-sdk-s3" % Versions.awsJavaSdkSecretManager
  )
}
