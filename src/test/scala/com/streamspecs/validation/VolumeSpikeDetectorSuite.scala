package com.streamspecs

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.VolumeSpikeRule
import com.streamspecs.domain.StatefulAlert
import com.streamspecs.validation.VolumeSpikeDetector
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class VolumeSpikeDetectorSuite extends CatsEffectSuite:

  test("State.record keeps only timestamps inside the window") {
    val state = VolumeSpikeDetector.State.empty
      .record(1000L, windowMs = 500)
      .record(1200L, windowMs = 500)
      .record(1600L, windowMs = 500) // drops 1000 (age 600 > 500)

    assertEquals(state.count, 2)
  }

  test("markEvent increments shared state") {
    for
      ref  <- Ref.of[IO, VolumeSpikeDetector.State](VolumeSpikeDetector.State.empty)
      _    <- VolumeSpikeDetector.markEvent(ref, windowMs = 60_000)
      _    <- VolumeSpikeDetector.markEvent(ref, windowMs = 60_000)
      _    <- VolumeSpikeDetector.markEvent(ref, windowMs = 60_000)
      snap <- ref.get
    yield assertEquals(snap.count, 3)
  }

  test("watchdog emits VolumeSpikeAnomaly when count exceeds max") {
    val rule = VolumeSpikeRule(
      metricKey = "alerts.stateful.volume_spike",
      windowDuration = 5.seconds,
      maxEventsInWindow = 3
    )
    for
      now <- IO.realTime.map(_.toMillis)
      // Pre-load 5 recent timestamps → already over max=3
      seeded = (0 until 5).foldLeft(VolumeSpikeDetector.State.empty) { (s, i) =>
        s.record(now - i * 10L, rule.windowDuration.toMillis)
      }
      ref <- Ref.of[IO, VolumeSpikeDetector.State](seeded)
      alert <- VolumeSpikeDetector
        .watchdog(ref, rule, pollInterval = 100.millis)
        .head
        .compile
        .lastOrError
        .timeout(3.seconds)
    yield alert match
      case StatefulAlert.VolumeSpikeAnomaly(count, max, key, windowMs) =>
        assert(count > max)
        assertEquals(max, 3)
        assertEquals(key, rule.metricKey)
        assertEquals(windowMs, 5000L)
      case other => fail(s"unexpected: $other")
    end for
  }

  test("watchdog stays silent when volume is below threshold") {
    val rule = VolumeSpikeRule("alerts.stateful.volume_spike", 2.seconds, maxEventsInWindow = 100)
    for
      ref <- Ref.of[IO, VolumeSpikeDetector.State](VolumeSpikeDetector.State.empty)
      _   <- VolumeSpikeDetector.markEvent(ref, rule.windowDuration.toMillis)
      alerts <- VolumeSpikeDetector
        .watchdog(ref, rule, pollInterval = 100.millis)
        .interruptAfter(500.millis)
        .compile
        .toList
    yield assertEquals(alerts, Nil)
  }

  test("watchdog is edge-triggered - one alert until cooled down") {
    val rule = VolumeSpikeRule("alerts.stateful.volume_spike", 5.seconds, maxEventsInWindow = 2)
    for
      now <- IO.realTime.map(_.toMillis)
      seeded = (0 until 5).foldLeft(VolumeSpikeDetector.State.empty) { (s, i) =>
        s.record(now - i, rule.windowDuration.toMillis)
      }
      ref <- Ref.of[IO, VolumeSpikeDetector.State](seeded)
      alerts <- VolumeSpikeDetector
        .watchdog(ref, rule, pollInterval = 80.millis)
        .interruptAfter(400.millis)
        .compile
        .toList
    yield assertEquals(alerts.length, 1)
    end for
  }
end VolumeSpikeDetectorSuite
