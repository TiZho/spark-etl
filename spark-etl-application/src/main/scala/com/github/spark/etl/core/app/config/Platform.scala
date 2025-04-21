package com.github.spark.etl.core.app.config

trait Platform {
  def protocol: String
  def path: Option[String]
  def relativeUri(implicit env: Env): String
}

object Platform {

  final case class FsPlatform(path: Option[String]) extends Platform {
    override val protocol: String = "file"
    override def relativeUri(implicit env: Env): String = path match {
      case Some(filepath) => s"$protocol://tmp/${env.value}/$filepath"
      case _              => s"$protocol://tmp/${env.value}"
    }
  }

  final case class S3Platform(bucketSuffix: String, path: Option[String]) extends Platform {
    override val protocol: String = "s3"
    override def relativeUri(implicit env: Env): String = path match {
      case Some(filepath) => s"$protocol://$bucketName/$filepath"
      case _              => s"$protocol://$bucketName"
    }
    def bucketName(implicit env: Env): String = s"${env.value.shortName}-$bucketSuffix"
  }

}
