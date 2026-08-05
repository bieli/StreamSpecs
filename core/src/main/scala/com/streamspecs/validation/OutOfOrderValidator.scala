package com.streamspecs.validation

import com.streamspecs.config.OutOfOrderRule
import com.streamspecs.core.*

object OutOfOrderValidator:

  final case class State(lastTimestamp: Option[Long])
  object State:
    val empty: State = State(None)

  def stage[T](rule: OutOfOrderRule)(using
      validator: DataQualityValidator[T]
  ): StatefulPipe.Stage[T] =
    _.mapAccumulate(State.empty) { (state, pair) =>
      val (outcome, prev) = pair
      extractTs(outcome) match
        case None => (state, (outcome, prev))
        case Some(ts) =>
          state.lastTimestamp match
            case Some(last) if ts < last =>
              val alert = StatefulAlert.OutOfOrderAnomaly(ts, last, rule.metricKey)
              val nextOutcome =
                if rule.sendToDlq then
                  EngineOutcome.Reject(
                    rawPayload = ts.toString,
                    issues = List(
                      RuleIssue(
                        "out-of-order-timestamp",
                        s"Event timestamp $ts is before last seen $last",
                        Severity.Error
                      )
                    ),
                    event = eventOpt(outcome)
                  )
                else
                  outcome match
                    case EngineOutcome.Pass(e) =>
                      EngineOutcome.PassWithWarnings(
                        e,
                        List(
                          RuleIssue(
                            "out-of-order-timestamp",
                            s"Out-of-order timestamp $ts < $last",
                            Severity.Warning
                          )
                        )
                      )
                    case EngineOutcome.PassWithWarnings(e, issues) =>
                      EngineOutcome.PassWithWarnings(
                        e,
                        issues :+ RuleIssue(
                          "out-of-order-timestamp",
                          s"Out-of-order timestamp $ts < $last",
                          Severity.Warning
                        )
                      )
                    case other => other
              (state, (nextOutcome, prev :+ alert))
            case _ =>
              (State(Some(ts)), (outcome, prev))
      end match
    }.map(_._2)

  private def extractTs[T](outcome: EngineOutcome[T])(using
      validator: DataQualityValidator[T]
  ): Option[Long] =
    eventOpt(outcome).flatMap(validator.extractTimestamp)

  private def eventOpt[T](outcome: EngineOutcome[T]): Option[T] =
    outcome match
      case EngineOutcome.Pass(e)                => Some(e)
      case EngineOutcome.PassWithWarnings(e, _) => Some(e)
      case EngineOutcome.Reject(_, _, e)        => e
end OutOfOrderValidator
