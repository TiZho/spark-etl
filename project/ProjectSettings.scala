import Depends.implicits
import sbt.Keys.test
import sbt.{Keys, ModuleID, addCommandAlias}
import sbtassembly.AssemblyKeys.{assembly, assemblyJarName}

object ProjectSettings {

  addCommandAlias("fmt", "scalafmt; Test / scalafmt;")
  addCommandAlias("check", "; scalafmtSbtCheck; scalafmtCheckAll;")

  // SETTINGS
  lazy val assemblySettings = Seq(
    assembly / assemblyJarName := Keys.name.value + ".jar",
    assembly / test            := {}
  )

  val baseDependencies: Seq[ModuleID] =
    Depends.pureconfig ++
      Depends.scopt ++
      Depends.monocle ++
      Depends.scalaLogging ++
      Depends.awsJavaSdkCore ++
      Depends.awsJavaSdkSecretManager ++
      Depends.awsJavaSdkS3 ++
      Depends.jackson ++
      Depends.betterfiles ++
      //Depends.shapeless.as("test") ++
      Depends.scalatest.as("test") ++
      Depends.h2Database.as("test")

  val commonDependencies: Seq[ModuleID] =
    baseDependencies ++
      Depends.spark.as("provided") ++
      Depends.delta.as("provided")

  val jdbcDependencies: Seq[ModuleID] =
    commonDependencies ++
      Depends.postgreSql
}
