package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.RollingAverageRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import fs2.Stream

/** Count-based rolling average check on successful / pass-through events. */
object RollingAverageValidator:

  def stage(rule: RollingAverageRule): StatefulPipe.Stage =
    _.mapAccumulate(RollingWindowState.empty) { (window, pair) =>
      val (outcome, prevAlerts) = pair
      outcome match
        case ValidationOutcome.Valid(event) =>
          evaluate(window, event, rule, outcome, prevAlerts)
        case ValidationOutcome.InvalidButPass(event, _, _, _) =>
          evaluate(window, event, rule, outcome, prevAlerts)
        case dlq: ValidationOutcome.DeadLetter =>
          (window, (dlq, prevAlerts))
    }.map(_._2)

  def pipe(
      rule: RollingAverageRule
  ): Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])] =
    in => stage(rule)(in.map(o => (o, Nil)))

  private def evaluate(
      window: RollingWindowState,
      event: TransactionEvent,
      rule: RollingAverageRule,
      outcome: ValidationOutcome,
      prevAlerts: List[StatefulAlert]
  ): (RollingWindowState, (ValidationOutcome, List[StatefulAlert])) =
    val next = window.add(event.price, rule.windowSizeEvents)
    val alert =
      if next.isFull(rule.windowSizeEvents) && next.average < rule.minAllowedAverage then
        List(
          StatefulAlert.RollingAverageAnomaly(
            currentAverage = next.average,
            threshold = rule.minAllowedAverage,
            metricKey = rule.metricKey,
            windowSize = rule.windowSizeEvents
          )
        )
      else Nil
    (next, (outcome, prevAlerts ++ alert))
  end evaluate
end RollingAverageValidator
