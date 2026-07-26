package com.streamspecs.validation

import cats.effect.IO
import com.streamspecs.config.DuplicateIdRule
import com.streamspecs.domain.{StatefulAlert, ValidationOutcome}
import fs2.Stream

import scala.collection.immutable.Queue

/** Detects repeated event IDs inside a sliding count window (Kafka retries / duplicates). */
object DuplicateIdValidator:

  final case class State(recentIds: Queue[String]):
    def remember(id: String, maxSize: Int): State =
      val next = recentIds.enqueue(id)
      if next.size > maxSize then State(next.dequeue._2) else State(next)

    def seen(id: String): Boolean = recentIds.contains(id)

  object State:
    val empty: State = State(Queue.empty)

  def stage(rule: DuplicateIdRule): StatefulPipe.Stage =
    _.mapAccumulate(State.empty) { (state, pair) =>
      val (outcome, prevAlerts) = pair
      extractId(outcome) match
        case None =>
          (state, (outcome, prevAlerts))
        case Some(id) if state.seen(id) =>
          val alert = StatefulAlert.DuplicateIdAnomaly(id, rule.metricKey)
          val nextOutcome =
            if rule.sendToDlq then
              ValidationOutcome.DeadLetter(
                rawPayload = id,
                errorCode = "duplicate-id",
                metricKey = rule.metricKey,
                reason = s"Duplicate event id '$id' within last ${rule.windowSizeEvents} events"
              )
            else
              outcome match
                case ValidationOutcome.Valid(e) =>
                  ValidationOutcome.InvalidButPass(
                    e,
                    "duplicate-id",
                    rule.metricKey,
                    s"Duplicate event id '$id'"
                  )
                case other => other
          (state.remember(id, rule.windowSizeEvents), (nextOutcome, prevAlerts ++ List(alert)))
        case Some(id) =>
          (state.remember(id, rule.windowSizeEvents), (outcome, prevAlerts))
      end match
    }.map(_._2)

  def pipe(
      rule: DuplicateIdRule
  ): Stream[IO, ValidationOutcome] => Stream[IO, (ValidationOutcome, List[StatefulAlert])] =
    in => stage(rule)(in.map(o => (o, Nil)))

  private def extractId(outcome: ValidationOutcome): Option[String] =
    outcome match
      case ValidationOutcome.Valid(e)                   => e.id.filter(_.nonEmpty)
      case ValidationOutcome.InvalidButPass(e, _, _, _) => e.id.filter(_.nonEmpty)
      case ValidationOutcome.DeadLetter(_, _, _, _)     => None
end DuplicateIdValidator
