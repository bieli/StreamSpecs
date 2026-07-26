package com.streamspecs

import com.streamspecs.config.DuplicateIdRule
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.DuplicateIdValidator
import fs2.Stream
import munit.CatsEffectSuite

class DuplicateIdValidatorSuite extends CatsEffectSuite:

  private def valid(id: String, price: Double = 10.0) =
    ValidationOutcome.Valid(TransactionEvent(Some(id), price, "a@b.com", Some("PLN"), None))

  private def dlq(raw: String = "x") =
    ValidationOutcome.DeadLetter(raw, "other", "m", "reason")

  test("duplicate id within window goes to DLQ when sendToDlq=true") {
    val rule =
      DuplicateIdRule("alerts.stateful.duplicate_id", windowSizeEvents = 10, sendToDlq = true)
    val events = Stream(valid("ORD-1"), valid("ORD-2"), valid("ORD-1"))

    DuplicateIdValidator.pipe(rule)(events).compile.toList.map { results =>
      results(2) match
        case (
              ValidationOutcome.DeadLetter(_, "duplicate-id", key, _),
              List(StatefulAlert.DuplicateIdAnomaly("ORD-1", _))
            ) =>
          assertEquals(key, rule.metricKey)
        case other => fail(s"unexpected: $other")
    }
  }

  test("duplicate id passes with warning when sendToDlq=false") {
    val rule =
      DuplicateIdRule("alerts.stateful.duplicate_id", windowSizeEvents = 10, sendToDlq = false)
    val events = Stream(valid("A"), valid("A"))

    DuplicateIdValidator.pipe(rule)(events).compile.toList.map { results =>
      results(1) match
        case (
              ValidationOutcome.InvalidButPass(_, "duplicate-id", _, _),
              List(_: StatefulAlert.DuplicateIdAnomaly)
            ) =>
          ()
        case other => fail(s"unexpected: $other")
    }
  }

  test("unique ids produce no alerts") {
    val rule   = DuplicateIdRule("m", windowSizeEvents = 10, sendToDlq = true)
    val events = Stream(valid("1"), valid("2"), valid("3"))

    DuplicateIdValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty))
      assert(results.forall(_._1.isInstanceOf[ValidationOutcome.Valid]))
    }
  }

  test("id falls out of window and can reappear without alert") {
    val rule = DuplicateIdRule("m", windowSizeEvents = 2, sendToDlq = true)
    // window size 2: after A,B,C - A is evicted, so second A is OK
    val events = Stream(valid("A"), valid("B"), valid("C"), valid("A"))

    DuplicateIdValidator.pipe(rule)(events).compile.toList.map { results =>
      assert(results.forall(_._2.isEmpty), s"alerts: ${results.flatMap(_._2)}")
    }
  }

  test("DeadLetter outcomes are skipped and do not update the window") {
    val rule   = DuplicateIdRule("m", windowSizeEvents = 10, sendToDlq = true)
    val events = Stream(dlq(), valid("X"), valid("X"))

    DuplicateIdValidator.pipe(rule)(events).compile.toList.map { results =>
      assertEquals(results.head._2, Nil)
      results(2) match
        case (ValidationOutcome.DeadLetter(_, "duplicate-id", _, _), List(_)) => ()
        case other => fail(s"expected duplicate on third event, got $other")
    }
  }
end DuplicateIdValidatorSuite
