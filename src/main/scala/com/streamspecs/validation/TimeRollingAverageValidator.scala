package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.TimeRollingAverageRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import fs2.Stream

/** Time-based rolling average: keep samples for the last `windowDuration` and alert when the mean
  * drops below `minAllowedAverage` (once enough samples are present).
  *
  * Timestamp source:
  *   - `eventTimestamp` when `useEventTimestamp=true` and the field is present
  *   - otherwise processing time (`Clock[IO].realTime`)
  */
object TimeRollingAverageValidator:

  def stage(rule: TimeRollingAverageRule): StatefulPipe.Stage =
    _.evalMapAccumulate(TimeRollingWindowState.empty) { (window, pair) =>
      val (outcome, prevAlerts) = pair
      outcome match
        case ValidationOutcome.Valid(event) =>
          evaluate(window, event, rule, outcome, prevAlerts)
        case ValidationOutcome.InvalidButPass(event, _, _, _) =>
          evaluate(window, event, rule, outcome, prevAlerts)
        case dlq: ValidationOutcome.DeadLetter =>
          IO.pure((window, (dlq, prevAlerts)))
    }.map(_._2)

  def pipe(
      rule: TimeRollingAverageRule
  ): Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])] =
    in => stage(rule)(in.map(o => (o, Nil)))

  private def evaluate(
      window: TimeRollingWindowState,
      event: TransactionEvent,
      rule: TimeRollingAverageRule,
      outcome: ValidationOutcome,
      prevAlerts: List[StatefulAlert]
  ): IO[(TimeRollingWindowState, (ValidationOutcome, List[StatefulAlert]))] =
    for
      nowMs <- IO.realTime.map(_.toMillis)
      ts    = resolveTimestamp(event, rule, nowMs)
      next  = window.add(ts, event.price, rule.windowDuration.toMillis)
      alert = maybeAlert(next, rule)
    yield (next, (outcome, prevAlerts ++ alert))

  private def resolveTimestamp(
      event: TransactionEvent,
      rule: TimeRollingAverageRule,
      nowMs: Long
  ): Long =
    if rule.useEventTimestamp then event.eventTimestamp.getOrElse(nowMs)
    else nowMs

  private def maybeAlert(
      window: TimeRollingWindowState,
      rule: TimeRollingAverageRule
  ): List[StatefulAlert] =
    if window.isReady(rule.minSamples) && window.average < rule.minAllowedAverage then
      List(
        StatefulAlert.TimeRollingAverageAnomaly(
          currentAverage = window.average,
          threshold = rule.minAllowedAverage,
          metricKey = rule.metricKey,
          windowMs = rule.windowDuration.toMillis,
          sampleCount = window.size
        )
      )
    else Nil
end TimeRollingAverageValidator
