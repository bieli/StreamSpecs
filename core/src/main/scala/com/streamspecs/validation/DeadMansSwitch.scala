package com.streamspecs.validation

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.HeartbeatRule
import com.streamspecs.core.StatefulAlert
import fs2.Stream

import scala.concurrent.duration.*

final case class StreamHeartbeat(lastSeenEpochMs: Long)

object DeadMansSwitch:

  def markAlive(ref: Ref[IO, StreamHeartbeat]): IO[Unit] =
    IO.realTime.map(_.toMillis).flatMap(now => ref.set(StreamHeartbeat(now)))

  def watchdog(
      state: Ref[IO, StreamHeartbeat],
      rule: HeartbeatRule,
      pollInterval: FiniteDuration = 1.second
  ): Stream[IO, StatefulAlert] =
    Stream.eval(Ref.of[IO, Boolean](false)).flatMap { alreadyAlerted =>
      Stream
        .awakeEvery[IO](pollInterval)
        .evalMap { _ =>
          for
            now <- IO.realTime.map(_.toMillis)
            hb  <- state.get
            idleMs = now - hb.lastSeenEpochMs
            idle   = idleMs.millis >= rule.maxIdleDuration
            fired <- alreadyAlerted.get
            alert <-
              if idle && !fired then
                alreadyAlerted.set(true) *>
                  IO.pure(
                    Some(
                      StatefulAlert.TemporalAnomaly(
                        reason = s"No events for ${idleMs}ms (threshold=${rule.maxIdleDuration})",
                        metricKey = rule.metricKey,
                        idleForMs = idleMs
                      )
                    )
                  )
              else if !idle && fired then alreadyAlerted.set(false) *> IO.pure(None)
              else IO.pure(None)
          yield alert
        }
        .unNone
    }
end DeadMansSwitch
