package com.streamspecs

import com.streamspecs.config.OutOfOrderRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.OutOfOrderValidator
import fs2.Stream
import munit.CatsEffectSuite

class OutOfOrderValidatorSuite extends CatsEffectSuite:

  private def valid(id: String, ts: Option[Long], price: Double = 10.0) =
    ValidationOutcome.Valid(TransactionEvent(Some(id), price, "a@b.com", Some("PLN"), ts))

  test("out-of-order timestamp emits warning when sendToDlq=false") {
    val rule = OutOfOrderRule("alerts.stateful.out_of_order", sendToDlq = false)
    val events = Stream(
      valid("1", Some(1000L)),
      valid("2", Some(2000L)),
      valid("3", Some(1500L))
    )

    OutOfOrderValidator.pipe(rule)(events).compile.toList.map { results =>
      results(2) match
        case (
              ValidationOutcome.InvalidButPass(_, "out-of-order-timestamp", _, _),
              List(StatefulAlert.OutOfOrderAnomaly(1500L, 2000L, _))
            ) =>
          ()
        case other => fail(s"unexpected: $other")
    }
  }

  test("out-of-order timestamp goes to DLQ when sendToDlq=true") {
    val rule   = OutOfOrderRule("alerts.stateful.out_of_order", sendToDlq = true)
    val events = Stream(valid("1", Some(1000L)), valid("2", Some(500L)))

    OutOfOrderValidator.pipe(rule)(events).compile.toList.map { results =>
      results(1) match
        case (
              ValidationOutcome.DeadLetter(_, "out-of-order-timestamp", _, _),
              List(_: StatefulAlert.OutOfOrderAnomaly)
            ) =>
          ()
        case other => fail(s"unexpected: $other")
    }
  }

  test("monotonically increasing timestamps produce no alerts") {
    val rule = OutOfOrderRule("m", sendToDlq = true)
    val events = Stream(
      valid("1", Some(100L)),
      valid("2", Some(200L)),
      valid("3", Some(200L)), // equal is OK (not strictly less)
      valid("4", Some(300L))
    )

    OutOfOrderValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("events without timestamp are ignored") {
    val rule = OutOfOrderRule("m", sendToDlq = true)
    val events = Stream(
      valid("1", None),
      valid("2", Some(1000L)),
      valid("3", None),
      valid("4", Some(2000L))
    )

    OutOfOrderValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("watermark does not advance on out-of-order so next in-order still works") {
    val rule = OutOfOrderRule("m", sendToDlq = false)
    val events = Stream(
      valid("1", Some(1000L)),
      valid("2", Some(500L)), // OOO - watermark stays at 1000
      valid("3", Some(1500L)) // in order vs 1000
    )

    OutOfOrderValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results(1)._2.nonEmpty)
      assertEquals(results(2)._2, Nil)
      assert(results(2)._1.isInstanceOf[ValidationOutcome.Valid])
    }
  }
end OutOfOrderValidatorSuite
