package com.streamspecs

import com.streamspecs.config.PriceDeviationRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.PriceDeviationValidator
import fs2.Stream
import munit.CatsEffectSuite

class PriceDeviationValidatorSuite extends CatsEffectSuite:

  private def valid(id: String, price: Double) =
    ValidationOutcome.Valid(TransactionEvent(Some(id), price, "a@b.com", Some("PLN"), None))

  test("fires on spike vs baseline when window is full") {
    val rule = PriceDeviationRule(
      "alerts.stateful.price_spike",
      windowSizeEvents = 3,
      maxDeviationPercent = 100.0
    )
    val events = Stream(
      valid("1", 10),
      valid("2", 10),
      valid("3", 10), // window full, avg=10
      valid("4", 50)  // 400% deviation
    )

    PriceDeviationValidator.pipe(rule)(events).compile.toList.map { results =>
      val alerts = results.flatMap(_._2)
      assertEquals(alerts.length, 1)
      alerts.head match
        case StatefulAlert.PriceDeviationAnomaly(price, baseline, pct, max, key) =>
          assertEquals(price, 50.0)
          assertEquals(baseline, 10.0)
          assert(pct > max)
          assertEquals(key, rule.metricKey)
        case other => fail(s"unexpected: $other")
    }
  }

  test("does not fire while window is still filling") {
    val rule   = PriceDeviationRule("m", windowSizeEvents = 5, maxDeviationPercent = 10.0)
    val events = Stream(valid("1", 10), valid("2", 1000)) // huge jump but window not full

    PriceDeviationValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("does not fire when deviation stays within threshold") {
    val rule = PriceDeviationRule("m", windowSizeEvents = 3, maxDeviationPercent = 50.0)
    val events = Stream(
      valid("1", 100),
      valid("2", 100),
      valid("3", 100),
      valid("4", 120) // 20% vs baseline 100 - under 50%
    )

    PriceDeviationValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty), s"got alerts: ${results.flatMap(_._2)}")
    }
  }

  test("DeadLetter outcomes do not update the baseline window") {
    val rule = PriceDeviationRule("m", windowSizeEvents = 3, maxDeviationPercent = 50.0)
    val dlq  = ValidationOutcome.DeadLetter("{}", "x", "m", "r")
    val events = Stream(
      valid("1", 10),
      valid("2", 10),
      valid("3", 10),
      dlq,
      valid("4", 11) // small change vs baseline 10 - no alert
    )

    PriceDeviationValidator.pipe(rule)(events).compile.toList.map { results =>
      assertEquals(results(3)._2, Nil)
      assert(results(4)._2.isEmpty)
    }
  }
end PriceDeviationValidatorSuite
