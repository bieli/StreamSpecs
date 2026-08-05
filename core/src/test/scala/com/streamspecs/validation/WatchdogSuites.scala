package com.streamspecs.validation

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.{HeartbeatRule, VolumeSpikeRule}
import com.streamspecs.core.StatefulAlert
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class WatchdogSuites extends CatsEffectSuite:

  test("volume spike state retains samples inside window only") {
    val windowMs = 500L
    val state =
      VolumeSpikeDetector.State.empty
        .record(1000L, windowMs)
        .record(1200L, windowMs)
        .record(1600L, windowMs) // drops 1000

    assertEquals(state.count, 2)
  }

  test("volume spike watchdog edge-triggers once while over threshold") {
    val rule = VolumeSpikeRule("alerts.stateful.volume_spike", 5.seconds, maxEventsInWindow = 2)
    for
      ref <- Ref[IO].of(VolumeSpikeDetector.State.empty)
      _   <- VolumeSpikeDetector.markEvent(ref, rule.windowDuration.toMillis)
      _   <- VolumeSpikeDetector.markEvent(ref, rule.windowDuration.toMillis)
      _   <- VolumeSpikeDetector.markEvent(ref, rule.windowDuration.toMillis)
      alerts <- VolumeSpikeDetector
        .watchdog(ref, rule, pollInterval = 50.millis)
        .take(1)
        .interruptAfter(3.seconds)
        .compile
        .toList
    yield
      assertEquals(alerts.length, 1)
      alerts.head match
        case StatefulAlert.VolumeSpikeAnomaly(count, 2, _, _) =>
          assert(count > 2)
        case other => fail(s"$other")
    end for
  }

  test("dead man's switch fires when idle beyond threshold") {
    val rule = HeartbeatRule("alerts.stateful.temporal", maxIdleDuration = 100.millis)
    for
      now <- IO.realTime.map(_.toMillis)
      ref <- Ref[IO].of(StreamHeartbeat(now - 5_000))
      alerts <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 50.millis)
        .take(1)
        .interruptAfter(3.seconds)
        .compile
        .toList
    yield
      assertEquals(alerts.length, 1)
      assert(alerts.head.isInstanceOf[StatefulAlert.TemporalAnomaly])
    end for
  }

  test("dead man's switch does not re-fire until traffic resumes then idles again") {
    val rule = HeartbeatRule("alerts.stateful.temporal", maxIdleDuration = 80.millis)
    for
      now0 <- IO.realTime.map(_.toMillis)
      ref  <- Ref[IO].of(StreamHeartbeat(now0 - 5_000))
      first <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 40.millis)
        .take(1)
        .interruptAfter(2.seconds)
        .compile
        .toList
      _ <- DeadMansSwitch.markAlive(ref)
      // still alive briefly - no second alert while fresh
      quiet <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 40.millis)
        .interruptAfter(60.millis)
        .compile
        .toList
      // wait past idle threshold without markAlive
      _ <- IO.sleep(120.millis)
      second <- DeadMansSwitch
        .watchdog(ref, rule, pollInterval = 40.millis)
        .take(1)
        .interruptAfter(2.seconds)
        .compile
        .toList
    yield
      assertEquals(first.length, 1)
      assertEquals(quiet, Nil)
      assertEquals(second.length, 1)
    end for
  }
end WatchdogSuites
