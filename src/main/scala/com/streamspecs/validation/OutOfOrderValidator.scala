package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.OutOfOrderRule
import com.streamspecs.domain.{StatefulAlert, ValidationOutcome}
import fs2.Stream

/** Detects out-of-order events: eventTimestamp strictly less than the last seen timestamp. */
object OutOfOrderValidator:

  final case class State(lastTimestamp: Option[Long])

  object State:
    val empty: State = State(None)

  def stage(rule: OutOfOrderRule): StatefulPipe.Stage =
    _.mapAccumulate(State.empty) { (state, pair) =>
      val (outcome, prevAlerts) = pair
      extractTs(outcome) match
        case None =>
          (state, (outcome, prevAlerts))
        case Some(ts) =>
          state.lastTimestamp match
            case Some(prev) if ts < prev =>
              val alert = StatefulAlert.OutOfOrderAnomaly(ts, prev, rule.metricKey)
              val nextOutcome =
                if rule.sendToDlq then
                  ValidationOutcome.DeadLetter(
                    rawPayload = ts.toString,
                    errorCode = "out-of-order-timestamp",
                    metricKey = rule.metricKey,
                    reason = s"Event timestamp $ts is before last seen $prev"
                  )
                else
                  outcome match
                    case ValidationOutcome.Valid(e) =>
                      ValidationOutcome.InvalidButPass(
                        e,
                        "out-of-order-timestamp",
                        rule.metricKey,
                        s"Out-of-order timestamp $ts < $prev"
                      )
                    case other => other
              (state, (nextOutcome, prevAlerts ++ List(alert)))
            case _ =>
              (State(Some(ts)), (outcome, prevAlerts))
      end match
    }.map(_._2)

  def pipe(
      rule: OutOfOrderRule
  ): Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])] =
    in => stage(rule)(in.map(o => (o, Nil)))

  private def extractTs(outcome: ValidationOutcome): Option[Long] =
    outcome match
      case ValidationOutcome.Valid(e)                   => e.eventTimestamp
      case ValidationOutcome.InvalidButPass(e, _, _, _) => e.eventTimestamp
      case ValidationOutcome.DeadLetter(_, _, _, _)     => None
end OutOfOrderValidator
