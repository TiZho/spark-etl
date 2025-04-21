package com.github.spark.etl.core.app.config

sealed trait Area {
  def service: String
  def name: String
  def fullname: String    = s"${service}_$name"
  def relativeUri: String = fullname
}

object Area {
  // type AreaLike = _ <: Area
  final case class Bronze(service: String) extends Area {
    lazy val name: String = Bronze.name
  }
  object Bronze {
    val name: String = "bronze"
  }
  final case class Silver(service: String) extends Area {
    lazy val name: String = Silver.name
  }

  object Silver {
    val name: String = "silver"
  }
  final case class Gold(service: String) extends Area {
    lazy val name: String = Gold.name
  }

  object Gold {
    val name: String = "gold"
  }

  def apply(name: String, service: String): Area = name.toLowerCase match {
    case Bronze.name => Bronze(service)
    case Silver.name => Silver(service)
    case Gold.name   => Gold(service)
    case _           => throw new IllegalArgumentException(s"Unknown area: $name")
  }
}
