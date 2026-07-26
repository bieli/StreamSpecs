package com.streamspecs

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.HeartbeatRule
import com.streamspecs.domain.StatefulAlert
import com.streamspecs.validation.{DeadMansSwitch, StreamHeartbeat}
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class DeadMansSwitchSuite extends CatsEffectSuite:

  test("watchdog emits TemporalAnomaly after idle threshold") {
    for
      now <- IO.realTime.map(_.toMillis)
      ref <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(now - 5000)) // already stale
      rule = HeartbeatRule("alerts.stateful.data_loss_detected", maxIdleDuration = 2.seconds)
      alert <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 200.millis)
        .head
        .compile
        .lastOrError
        .timeout(3.seconds)
    yield alert match
      case StatefulAlert.TemporalAnomaly(_, key, idle) =>
        assertEquals(key, rule.metricKey)
        assert(idle >= 2000)
      case other => fail(s"unexpected: $other")
  }

  test("markAlive prevents immediate alert") {
    for
      now <- IO.realTime.map(_.toMillis)
      ref <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(now))
      _   <- DeadMansSwitch.markAlive(ref)
      rule = HeartbeatRule("alerts.stateful.data_loss_detected", maxIdleDuration = 5.seconds)
      alerts <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 200.millis)
        .interruptAfter(800.millis)
        .compile
        .toList
    yield assertEquals(alerts, Nil)
  }

  test("watchdog is edge-triggered - one alert per silence gap") {
    for
      now <- IO.realTime.map(_.toMillis)
      ref <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(now - 10_000))
      rule = HeartbeatRule("alerts.stateful.data_loss_detected", maxIdleDuration = 1.second)
      alerts <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 100.millis)
        .interruptAfter(500.millis)
        .compile
        .toList
    yield assertEquals(alerts.length, 1)
  }

  test("markAlive updates heartbeat timestamp") {
    for
      ref   <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(0L))
      _     <- DeadMansSwitch.markAlive(ref)
      after <- ref.get
      now   <- IO.realTime.map(_.toMillis)
    yield assert(math.abs(after.lastSeenEpochMs - now) < 2000)
  }
end DeadMansSwitchSuite
