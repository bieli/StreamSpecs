package com.streamspecs.validation

import com.streamspecs.config.RollingAverageRule
import com.streamspecs.core.*

object RollingAverageValidator:

  def stage[T](rule: RollingAverageRule)(using
      validator: DataQualityValidator[T]
  ): StatefulPipe.Stage[T] =
    _.mapAccumulate(RollingWindowState.empty) { (window, pair) =>
      val (outcome, prev) = pair
      metricValue(outcome, rule.metricName) match
        case None => (window, (outcome, prev))
        case Some(value) =>
          val next = window.add(value, rule.windowSizeEvents)
          val alert =
            if next.isFull(rule.windowSizeEvents) && next.average < rule.minAllowedAverage then
              List(
                StatefulAlert.RollingAverageAnomaly(
                  metricName = rule.metricName,
                  currentAverage = next.average,
                  threshold = rule.minAllowedAverage,
                  metricKey = rule.metricKey,
                  windowSize = rule.windowSizeEvents
                )
              )
            else Nil
          (next, (outcome, prev ++ alert))
      end match
    }.map(_._2)

  private def metricValue[T](outcome: EngineOutcome[T], name: String)(using
      validator: DataQualityValidator[T]
  ): Option[Double] =
    outcome match
      case EngineOutcome.Pass(e)                => validator.extractMetricValue(e, name)
      case EngineOutcome.PassWithWarnings(e, _) => validator.extractMetricValue(e, name)
      case EngineOutcome.Reject(_, _, _)        => None
end RollingAverageValidator
