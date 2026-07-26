package com.streamspecs

import com.streamspecs.config.TimeRollingAverageRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.{TimeRollingAverageValidator, TimeRollingWindowState}
import fs2.Stream
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class TimeRollingAverageValidatorSuite extends CatsEffectSuite:

  private def valid(id: String, price: Double, ts: Long) =
    ValidationOutcome.Valid(
      TransactionEvent(Some(id), price, "a@b.com", Some("PLN"), Some(ts))
    )

  private val rule = TimeRollingAverageRule(
    metricKey = "alerts.stateful.time_rolling_low_average",
    field = "price",
    windowDuration = 1.second,
    minAllowedAverage = 50.0,
    minSamples = 2,
    useEventTimestamp = true
  )

  test("time window expires old samples and keeps only recent ones") {
    val state =
      TimeRollingWindowState.empty
        .add(1000L, 100.0, windowMs = 500)
        .add(1200L, 80.0, windowMs = 500)
        .add(1600L, 10.0, windowMs = 500) // drops 1000 (age 600 > 500)

    assertEquals(state.size, 2)
    assertEquals(state.average, (80.0 + 10.0) / 2.0)
  }

  test("alerts when time-window average falls below threshold") {
    val t0 = 1_000_000L
    val events = Stream(
      valid("1", 100.0, t0),
      valid("2", 10.0, t0 + 100), // avg=55 still OK
      valid("3", 5.0, t0 + 200)   // avg≈38.3 < 50 → alert
    )

    TimeRollingAverageValidator.pipe(rule)(events).compile.toList.map { results =>
      val alerts = results.flatMap(_._2)
      assertEquals(alerts.length, 1)
      alerts.head match
        case StatefulAlert.TimeRollingAverageAnomaly(avg, threshold, key, windowMs, n) =>
          assert(avg < threshold)
          assertEquals(threshold, 50.0)
          assertEquals(key, rule.metricKey)
          assertEquals(windowMs, 1000L)
          assertEquals(n, 3)
        case other => fail(s"unexpected: $other")
    }
  }

  test("does not alert before minSamples") {
    val sparse = rule.copy(minSamples = 5)
    val t0     = 2_000_000L
    val events = Stream(valid("1", 1.0, t0), valid("2", 1.0, t0 + 10))

    TimeRollingAverageValidator.pipe(sparse)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("old samples leave the window so average can recover") {
    val t0 = 3_000_000L
    val events = Stream(
      valid("1", 1.0, t0),       // low
      valid("2", 1.0, t0 + 100), // still low → alert (n=2, avg=1)
      valid(
        "3",
        100.0,
        t0 + 1500
      ) // 1s window → only last sample remains, n=1 < minSamples → no alert
    )

    TimeRollingAverageValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results(1)._2.nonEmpty, "expected alert on second event")
      assertEquals(results(2)._2, Nil)
    }
  }

  test("DeadLetter does not update the time window") {
    val t0  = 4_000_000L
    val dlq = ValidationOutcome.DeadLetter("{}", "x", "m", "r")
    val events = Stream(
      valid("1", 100.0, t0),
      dlq,
      valid("2", 100.0, t0 + 50)
    )

    TimeRollingAverageValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("falls back to processing time when eventTimestamp missing") {
    val processingTimeRule =
      rule.copy(useEventTimestamp = true, minSamples = 1, minAllowedAverage = 1000.0)
    val event =
      ValidationOutcome.Valid(TransactionEvent(Some("1"), 1.0, "a@b.com", Some("PLN"), None))

    TimeRollingAverageValidator.pipe(processingTimeRule)(Stream(event)).compile.toList.map {
      results =>
        // single sample, minSamples=1, avg=1 < 1000 → alert
        assert(results.head._2.exists(_.isInstanceOf[StatefulAlert.TimeRollingAverageAnomaly]))
    }
  }
end TimeRollingAverageValidatorSuite
