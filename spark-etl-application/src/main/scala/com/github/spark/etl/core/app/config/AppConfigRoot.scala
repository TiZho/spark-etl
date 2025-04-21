package com.github.spark.etl.core.app.config

import com.github.spark.etl.core.app.config

trait AppConfigRoot {
  def appName: String
  def stageName: String
  def envName: String
  def debugMode: Boolean
  def dataframeEnabled: Boolean
  def source: Source
  def dest: Dest
  def env: Env = envName match {
    case Env.Local.name       => config.Env(Env.Local)
    case Env.Development.name => config.Env(Env.Development)
    case Env.Staging.name     => config.Env(Env.Staging)
    case Env.Production.name  => config.Env(Env.Production)
    case env =>
      throw new IllegalArgumentException(
        s"can not parse environment $env: (should be local, development, staging, production) "
      )
  }
}
