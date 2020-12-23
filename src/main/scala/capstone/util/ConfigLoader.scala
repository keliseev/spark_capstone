package scala.capstone.util

import com.typesafe.config.{Config, ConfigFactory}

object ConfigLoader {
  val config: Config = ConfigFactory.load("application.conf").getConfig("demoApp")
  val sessionsConfig: Config = config.getConfig("sessions")
  val purchasesConfig: Config = config.getConfig("purchases")
  val projectionConfig: Config = config.getConfig("projections")
}
