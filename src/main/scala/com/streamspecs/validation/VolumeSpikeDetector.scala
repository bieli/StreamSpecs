package com.streamspecs.validation

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.VolumeSpikeRule
import com.streamspecs.domain.StatefulAlert
import fs2.Stream

import scala.collection.immutable.Queue
import scala.concurrent.duration.*

/** Sliding time-window volume check: alert when event rate spikes above threshold. Pair with
  * [[DeadMansSwitch]] (too quiet) - this catches "too loud".
  */
object VolumeSpikeDetector:

  final case class State(timestampsMs: Queue[Long]):
    def record(nowMs: Long, windowMs: Long): State =
      val trimmed = timestampsMs.enqueue(nowMs).filter(t => nowMs - t <= windowMs)
      State(trimmed)

    def count: Int = timestampsMs.size

  object State:
    val empty: State = State(Queue.empty)

  /** Call on every processed event to update the shared window. */
  def markEvent(ref: Ref[IO, State], windowMs: Long): IO[Unit] =
    IO.realTime.map(_.toMillis).flatMap { now =>
      ref.update(_.record(now, windowMs))
    }

  /** Background poller that emits a spike alert when count exceeds max. Edge-triggered: one alert
    * until the window cools below the threshold.
    */
  def watchdog(
      state: Ref[IO, State],
      rule: VolumeSpikeRule,
      pollInterval: FiniteDuration = 500.millis
  ): Stream[IO, StatefulAlert] =
    val windowMs = rule.windowDuration.toMillis
    Stream.eval(Ref.of[IO, Boolean](false)).flatMap { alreadyAlerted =>
      Stream
        .awakeEvery[IO](pollInterval)
        .evalMap { _ =>
          for
            now  <- IO.realTime.map(_.toMillis)
            _    <- state.update(_.record(now, windowMs)) // age out stale timestamps
            snap <- state.get
            over = snap.count > rule.maxEventsInWindow
            fired <- alreadyAlerted.get
            alert <-
              if over && !fired then
                alreadyAlerted.set(true) *>
                  IO.pure(
                    Some(
                      StatefulAlert.VolumeSpikeAnomaly(
                        countInWindow = snap.count,
                        maxAllowed = rule.maxEventsInWindow,
                        metricKey = rule.metricKey,
                        windowMs = windowMs
                      )
                    )
                  )
              else if !over && fired then alreadyAlerted.set(false) *> IO.pure(None)
              else IO.pure(None)
          yield alert
        }
        .unNone
    }
  end watchdog
end VolumeSpikeDetector
