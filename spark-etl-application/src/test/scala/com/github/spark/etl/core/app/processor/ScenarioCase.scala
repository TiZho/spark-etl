package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.AppConfig

sealed trait ScenarioCase[In, Out, Conf <: AppConfig] {
  def input: In
  def configuration: Conf
  def caseTitle: String
  def caseDescription: String
}

object ScenarioCase {

  sealed trait AbstractCompareScenarioCase[In, Out, Conf <: AppConfig]
      extends ScenarioCase[In, Out, Conf] {
    def expected: Out
  }

  sealed trait AbstractCheckScenarioCase[In, Out, Conf <: AppConfig]
      extends ScenarioCase[In, Out, Conf] {
    def checkCondition: Out => Boolean
  }

  sealed trait AbstractErrorScenarioCase[In, Out, Err <: Throwable, Conf <: AppConfig]
      extends ScenarioCase[In, Out, Conf] {
    def returningError: Err
    def errorMessage: String
  }

  final case class CompareScenarioCase[In, Out, Conf <: AppConfig](
      input: In,
      configuration: Conf,
      expected: Out,
      caseTitle: String,
      caseDescription: String)
      extends AbstractCompareScenarioCase[In, Out, Conf]

  final case class CheckScenarioCase[In, Out, Conf <: AppConfig](
      input: In,
      configuration: Conf,
      checkCondition: Out => Boolean,
      caseTitle: String,
      caseDescription: String)
      extends AbstractCheckScenarioCase[In, Out, Conf]

  final case class ErrorScenarioCase[In, Out, Err <: Throwable, Conf <: AppConfig](
      input: In,
      configuration: Conf,
      returningError: Err,
      caseTitle: String,
      caseDescription: String,
      errorMessage: String)
      extends AbstractErrorScenarioCase[In, Out, Err, Conf]

}
