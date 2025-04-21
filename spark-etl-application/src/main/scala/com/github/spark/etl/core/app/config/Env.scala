package com.github.spark.etl.core.app.config

import Env.EnvType

final case class Env(value: EnvType)

object Env {

  sealed trait EnvType {
    def name: String
    def shortName: String
  }

  case object Local extends EnvType {
    override val name: String      = "local"
    override val shortName: String = "loc"
  }

  case object Development extends EnvType {
    override val name: String      = "development"
    override val shortName: String = "dev"
  }

  case object Staging extends EnvType {
    override val name: String      = "staging"
    override val shortName: String = "sta"
  }

  case object Production extends EnvType {
    override val name: String      = "production"
    override val shortName: String = "prd"
  }

}
