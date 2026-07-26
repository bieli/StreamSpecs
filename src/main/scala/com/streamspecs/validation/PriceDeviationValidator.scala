package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.PriceDeviationRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import fs2.Stream

/** Detects sudden price spikes vs the rolling baseline average. */
object PriceDeviationValidator:

  def stage(rule: PriceDeviationRule): StatefulPipe.Stage =
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
      rule: PriceDeviationRule
  ): Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])] =
    in => stage(rule)(in.map(o => (o, Nil)))

  private def evaluate(
      window: RollingWindowState,
      event: TransactionEvent,
      rule: PriceDeviationRule,
      outcome: ValidationOutcome,
      prevAlerts: List[StatefulAlert]
  ): (RollingWindowState, (ValidationOutcome, List[StatefulAlert])) =
    val baseline = window.average
    val next     = window.add(event.price, rule.windowSizeEvents)
    val alert =
      if window.isFull(rule.windowSizeEvents) && baseline > 0 then
        val deviationPct = math.abs(event.price - baseline) / baseline * 100.0
        if deviationPct > rule.maxDeviationPercent then
          List(
            StatefulAlert.PriceDeviationAnomaly(
              price = event.price,
              baselineAverage = baseline,
              deviationPercent = deviationPct,
              maxAllowedPercent = rule.maxDeviationPercent,
              metricKey = rule.metricKey
            )
          )
        else Nil
        end if
      else Nil
    (next, (outcome, prevAlerts ++ alert))
  end evaluate
end PriceDeviationValidator
