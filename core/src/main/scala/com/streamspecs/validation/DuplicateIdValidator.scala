package com.streamspecs.validation

import com.streamspecs.config.DuplicateIdRule
import com.streamspecs.core.*

import scala.collection.immutable.Queue

object DuplicateIdValidator:

  final case class State(recentIds: Queue[String]):
    def remember(id: String, maxSize: Int): State =
      val next = recentIds.enqueue(id)
      if next.size > maxSize then State(next.dequeue._2) else State(next)
    def seen(id: String): Boolean = recentIds.contains(id)

  object State:
    val empty: State = State(Queue.empty)

  def stage[T](rule: DuplicateIdRule)(using
      validator: DataQualityValidator[T]
  ): StatefulPipe.Stage[T] =
    _.mapAccumulate(State.empty) { (state, pair) =>
      val (outcome, prev) = pair
      extractId(outcome) match
        case None => (state, (outcome, prev))
        case Some(id) if state.seen(id) =>
          val alert = StatefulAlert.DuplicateIdAnomaly(id, rule.metricKey)
          val nextOutcome =
            if rule.sendToDlq then
              EngineOutcome.Reject(
                rawPayload = id,
                issues = List(
                  RuleIssue(
                    "duplicate-id",
                    s"Duplicate event id '$id' within last ${rule.windowSizeEvents} events",
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
                    List(RuleIssue("duplicate-id", s"Duplicate event id '$id'", Severity.Warning))
                  )
                case EngineOutcome.PassWithWarnings(e, issues) =>
                  EngineOutcome.PassWithWarnings(
                    e,
                    issues :+ RuleIssue(
                      "duplicate-id",
                      s"Duplicate event id '$id'",
                      Severity.Warning
                    )
                  )
                case other => other
          (state.remember(id, rule.windowSizeEvents), (nextOutcome, prev :+ alert))
        case Some(id) =>
          (state.remember(id, rule.windowSizeEvents), (outcome, prev))
      end match
    }.map(_._2)

  private def extractId[T](outcome: EngineOutcome[T])(using
      validator: DataQualityValidator[T]
  ): Option[String] =
    eventOpt(outcome).flatMap(validator.extractId)

  private def eventOpt[T](outcome: EngineOutcome[T]): Option[T] =
    outcome match
      case EngineOutcome.Pass(e)                => Some(e)
      case EngineOutcome.PassWithWarnings(e, _) => Some(e)
      case EngineOutcome.Reject(_, _, e)        => e
end DuplicateIdValidator
