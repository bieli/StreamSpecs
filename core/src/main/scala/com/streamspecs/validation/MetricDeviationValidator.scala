package com.streamspecs.validation

import com.streamspecs.config.MetricDeviationRule
import com.streamspecs.core.*

object MetricDeviationValidator:

  def stage[T](rule: MetricDeviationRule)(using
      validator: DataQualityValidator[T]
  ): StatefulPipe.Stage[T] =
    _.mapAccumulate(RollingWindowState.empty) { (window, pair) =>
      val (outcome, prev) = pair
      metricValue(outcome, rule.metricName) match
        case None => (window, (outcome, prev))
        case Some(value) =>
          val baseline = window.average
          val next     = window.add(value, rule.windowSizeEvents)
          val alert =
            if window.isFull(rule.windowSizeEvents) && baseline > 0 then
              val deviationPct = math.abs(value - baseline) / baseline * 100.0
              if deviationPct > rule.maxDeviationPercent then
                List(
                  StatefulAlert.MetricDeviationAnomaly(
                    metricName = rule.metricName,
                    value = value,
                    baselineAverage = baseline,
                    deviationPercent = deviationPct,
                    maxAllowedPercent = rule.maxDeviationPercent,
                    metricKey = rule.metricKey
                  )
                )
              else Nil
              end if
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
end MetricDeviationValidator
