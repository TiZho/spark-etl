import ProjectSettings._
import Common._

addCommandAlias("fmt", "scalafmt; Test / scalafmt;")
addCommandAlias("check", "; scalafmtSbtCheck; scalafmtCheckAll;")

lazy val coreMacros =
  Project("spark-etl-macros", file("./spark-etl-macros"))
    .settings(Common.settings())
    .settings(
      assemblySettings,
      name := "spark-etl-macros",
      libraryDependencies ++= commonDependencies
    )

lazy val coreApplication =
  Project("spark-etl-application", file("./spark-etl-application"))
    .settings(Common.settings())
    .settings(
      assemblySettings,
      name := "spark-etl-application",
      libraryDependencies ++= commonDependencies
    )
    .dependsOn(coreMacros % "compile->compile;test->test")

lazy val coreExamples =
  Project("spark-etl-examples", file("./spark-etl-examples"))
    .settings(settings())
    .settings(
      assemblySettings,
      name := "spark-etl-examples",
      libraryDependencies ++= commonDependencies
    )
    .dependsOn(coreApplication % "compile->compile;test->test")

