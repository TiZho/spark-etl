import sbt.Keys.*
import sbt.*
import sbtassembly.AssemblyKeys.*
import sbtassembly.AssemblyPlugin.autoImport.{ MergeStrategy, PathList, ShadeRule }

object Common {
  val projectScalaVersion = Versions.scala
  def settings(scalaTags: Boolean = true) = Seq(
    organization := "com.symphony.royalties",
    scalaVersion := projectScalaVersion,
    ThisBuild / useCoursier := false, // Disabling coursier fixes the problem with java.lang.NoClassDefFoundError: scala/xml while
    // publishing child modules: https://github.com/sbt/sbt/issues/4995

    isSnapshot := false,
    version    := "master",
    resolvers += DefaultMavenRepository,
    resolvers += Resolver.mavenLocal,
    credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x                             => MergeStrategy.first
    },
    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll
    ),
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint"),
    Test / scalacOptions ++= Seq("-Yrangepos"),
    Test / logBuffered       := false,
    Test / parallelExecution := false,
    scalacOptions ++= ScalaSettings.options
  ) ++ {
    if (scalaTags)
      Seq(
        scalacOptions ++= Seq(
          "-encoding",
          "UTF-8",
          "-target:jvm-1.8",
          "-deprecation",
          "-feature",
          "-unchecked",
          "-Xlint",
          "-Yno-adapted-args"
        )
      )
    else
      Seq()
  }
}
