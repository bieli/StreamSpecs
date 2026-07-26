package com.streamspecs

import com.streamspecs.config.{DuplicateIdRule, OutOfOrderRule}
import com.streamspecs.domain.{StatefulAlert, TransactionEvent, ValidationOutcome}
import com.streamspecs.validation.{DuplicateIdValidator, OutOfOrderValidator, StatefulPipe}
import fs2.Stream
import munit.CatsEffectSuite

class StatefulPipeSuite extends CatsEffectSuite:

  private def valid(id: String, price: Double, ts: Option[Long] = None) =
    ValidationOutcome.Valid(TransactionEvent(Some(id), price, "a@b.com", Some("PLN"), ts))

  test("identityLegacy passes outcomes with empty alerts") {
    val events = Stream(valid("1", 10), valid("2", 20))
    StatefulPipe.identityLegacy(events).compile.toList.map { results =>
      assertEquals(results.map(_._1), List(valid("1", 10), valid("2", 20)))
      assert(results.forall(_._2.isEmpty))
    }
  }

  test("liftOption(None) behaves like identity stage") {
    val events = Stream((valid("1", 10), List.empty[StatefulAlert]))
    StatefulPipe.liftOption(None)(events).compile.toList.map { results =>
      assertEquals(results.length, 1)
      assertEquals(results.head._2, Nil)
    }
  }

  test("sequence concatenates alerts from chained validators with preserved state") {
    val dupRule = DuplicateIdRule("dup", windowSizeEvents = 10, sendToDlq = false)
    val oooRule = OutOfOrderRule("ooo", sendToDlq = false)

    val pipe = StatefulPipe.sequence(
      List(
        DuplicateIdValidator.stage(dupRule),
        OutOfOrderValidator.stage(oooRule)
      )
    )

    val events = Stream(
      valid("A", 10, Some(1000L)),
      valid("B", 10, Some(2000L)),
      valid("A", 10, Some(1500L)) // duplicate + out-of-order
    )

    pipe(events).compile.toList.map { results =>
      val alerts = results(2)._2
      assert(
        alerts.exists(_.isInstanceOf[StatefulAlert.DuplicateIdAnomaly]),
        s"missing duplicate alert: $alerts"
      )
      assert(
        alerts.exists(_.isInstanceOf[StatefulAlert.OutOfOrderAnomaly]),
        s"missing OOO alert: $alerts"
      )
      assertEquals(alerts.length, 2)
    }
  }

  test("empty sequence is identity") {
    val events = Stream(valid("1", 1.0))
    StatefulPipe.sequence(Nil)(events).compile.toList.map { results =>
      assertEquals(results.head._1, valid("1", 1.0))
      assertEquals(results.head._2, Nil)
    }
  }
end StatefulPipeSuite
