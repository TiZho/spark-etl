package com.github.spark.etl.examples.config

import com.github.spark.etl.core.app.config.{AppConfig, AppConfigRoot}
import com.github.spark.etl.core.app.config.Dest.SilverDest
import com.github.spark.etl.core.app.config.Source.BronzeSource
import com.github.spark.etl.examples.config.SalesConfig.SalesConfigRoot


final case class SalesConfig(app: SalesConfigRoot) extends AppConfig

object SalesConfig {
  final case class SalesConfigRoot(appName: String,
                                   stageName: String,
                                   debugMode: Boolean,
                                   dataframeEnabled: Boolean,
                                   envName: String,
                                   source: BronzeSource,
                                   dest: SilverDest) extends AppConfigRoot
}