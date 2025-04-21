package com.github.spark.etl.core.app.config

import better.files.File
import com.amazonaws.regions.Regions
import com.github.spark.etl.core.app.utils.Secret.StringSecret
import com.github.spark.etl.core.app.utils.SimpleJsonParser._

import java.util.Properties

trait DatabaseSource extends Datasource {
  def tablename: String
  def user: String
  def password: String
  def hostname: String
  def port: Int
  def database: String
  def databaseType: String
  def driverType: String
  def readOptions(implicit env: Env): Map[String, String] =
    Map(
      "user"     -> user,
      "password" -> password,
      "driver"   -> driverType,
      "dbtable"  -> tablename,
      "url"      -> uri
    )

  def properties(implicit env: Env): Properties = {
    val props = new Properties
    readOptions.foreach { case (key, value) =>
      props.setProperty(key, value)
    }
    props
  }
}

object DatabaseSource {
  final case class DefaultDatabaseSource(
      tablename: String,
      secretKey: StringSecret,
      regionKey: String,
      databaseType: String,
      driverType: String)
      extends DatabaseSource {
    import com.github.spark.etl.core.app.utils.AwsSecretUtils._
    import com.github.spark.etl.core.app.utils.AwsUtils._
    override def uri(implicit env: Env): String = env.value match {
      case Env.Local =>
        val subdirectory: File = File("./data/h2/")
        subdirectory.createDirectories()
        val databaseFilePath = (subdirectory / database).pathAsString
        s"jdbc:$databaseType:$databaseFilePath"
      case _ => s"jdbc:$databaseType://$hostname:$port/$database"
    }

    override def datasourceType: DatasourceType = DatasourceType.Jdbc
    def region: Regions                         = loadRegionStrict(regionKey)
    lazy val json: String                       = readStrictSecret(secretKey, region)

    def database: String = extractString(json, "database")
      .getOrElse(throw new IllegalArgumentException("database not found"))

    def user: String = extractString(json, "username")
      .getOrElse(throw new IllegalArgumentException("username not found"))

    def password: String = extractString(json, "password")
      .getOrElse(throw new IllegalArgumentException("password not found"))

    def hostname: String = extractString(json, "host")
      .getOrElse(throw new IllegalArgumentException("host not found"))

    def port: Int = extractNumber(json, "port")
      .getOrElse(throw new IllegalArgumentException("port not found"))
  }
}
