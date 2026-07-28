package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.TimeRollingAverageRule
import com.streamspecs.core.*

object TimeRollingAverageValidator:

  def stage[T](rule: TimeRollingAverageRule)(using
      validator: DataQualityValidator[T]
  ): StatefulPipe.Stage[T] =
    _.evalMapAccumulate(TimeRollingWindowState.empty) { (window, pair) =>
      val (outcome, prev) = pair
      metricAndTs(outcome, rule) match
        case None => IO.pure((window, (outcome, prev)))
        case Some((value, tsOpt)) =>
          for
            now <- IO.realTime.map(_.toMillis)
            ts   = if rule.useEventTimestamp then tsOpt.getOrElse(now) else now
            next = window.add(ts, value, rule.windowDuration.toMillis)
            alert =
              if next.isReady(rule.minSamples) && next.average < rule.minAllowedAverage then
                List(
                  StatefulAlert.TimeRollingAverageAnomaly(
                    metricName = rule.metricName,
                    currentAverage = next.average,
                    threshold = rule.minAllowedAverage,
                    metricKey = rule.metricKey,
                    windowMs = rule.windowDuration.toMillis,
                    sampleCount = next.size
                  )
                )
              else Nil
          yield (next, (outcome, prev ++ alert))
      end match
    }.map(_._2)

  private def metricAndTs[T](outcome: EngineOutcome[T], rule: TimeRollingAverageRule)(using
      validator: DataQualityValidator[T]
  ): Option[(Double, Option[Long])] =
    outcome match
      case EngineOutcome.Pass(e) =>
        validator.extractMetricValue(e, rule.metricName).map(_ -> validator.extractTimestamp(e))
      case EngineOutcome.PassWithWarnings(e, _) =>
        validator.extractMetricValue(e, rule.metricName).map(_ -> validator.extractTimestamp(e))
      case EngineOutcome.Reject(_, _, _) => None
end TimeRollingAverageValidator
