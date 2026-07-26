package com.streamspecs

import com.streamspecs.config.RollingAverageRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.{RollingAverageValidator, RollingWindowState}
import fs2.Stream
import munit.CatsEffectSuite

class RollingWindowSuite extends CatsEffectSuite:

  test("rolling window trims to max size and computes average") {
    val state =
      RollingWindowState.empty
        .add(100.0, 3)
        .add(50.0, 3)
        .add(30.0, 3)
        .add(10.0, 3) // drops 100 → window=[50,30,10]

    assertEquals(state.size, 3)
    assertEquals(state.average, (50.0 + 30.0 + 10.0) / 3.0)
  }

  test("rolling average anomaly fires when window is full and below threshold") {
    val rule = RollingAverageRule(
      metricKey = "alerts.stateful.low_rolling_average",
      field = "price",
      windowSizeEvents = 3,
      minAllowedAverage = 50.0
    )

    val events = Stream.emits(
      List(
        TransactionEvent(Some("1"), 100.0, "a@b.com"),
        TransactionEvent(Some("2"), 60.0, "a@b.com"),
        TransactionEvent(Some("3"), 55.0, "a@b.com"),
        TransactionEvent(Some("4"), 10.0, "a@b.com"), // avg(60,55,10)=41.66 → alert
        TransactionEvent(Some("5"), 15.0, "a@b.com")  // avg(55,10,15)=26.66 → alert
      ).map(ValidationOutcome.Valid(_))
    )

    RollingAverageValidator
      .pipe(rule)(events)
      .compile
      .toList
      .map { results =>
        val alerts = results.flatMap(_._2)
        assertEquals(alerts.size, 2)
        alerts.foreach {
          case StatefulAlert.RollingAverageAnomaly(avg, threshold, key, window) =>
            assert(avg < threshold)
            assertEquals(threshold, 50.0)
            assertEquals(key, rule.metricKey)
            assertEquals(window, 3)
          case other => fail(s"unexpected alert: $other")
        }
      }
  }

  test("rolling average stays quiet when mean is above threshold") {
    val rule = RollingAverageRule("m", "price", windowSizeEvents = 3, minAllowedAverage = 10.0)
    val events = Stream(
      ValidationOutcome.Valid(TransactionEvent(Some("1"), 100.0, "a@b.com")),
      ValidationOutcome.Valid(TransactionEvent(Some("2"), 80.0, "a@b.com")),
      ValidationOutcome.Valid(TransactionEvent(Some("3"), 60.0, "a@b.com"))
    )

    RollingAverageValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("empty rolling window average is zero") {
    assertEquals(RollingWindowState.empty.average, 0.0)
    assertEquals(RollingWindowState.empty.size, 0)
    assert(!RollingWindowState.empty.isFull(3))
  }

  test("DeadLetter does not affect rolling average state") {
    val rule = RollingAverageRule("m", "price", 3, 50.0)
    val events = Stream(
      ValidationOutcome.Valid(TransactionEvent(Some("1"), 100.0, "a@b.com")),
      ValidationOutcome.DeadLetter("{}", "x", "m", "r"),
      ValidationOutcome.Valid(TransactionEvent(Some("2"), 100.0, "a@b.com")),
      ValidationOutcome.Valid(TransactionEvent(Some("3"), 100.0, "a@b.com"))
    )

    RollingAverageValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }
end RollingWindowSuite
