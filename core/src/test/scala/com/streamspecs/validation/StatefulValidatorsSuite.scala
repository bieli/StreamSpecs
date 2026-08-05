package com.streamspecs.validation

import com.streamspecs.config.*
import com.streamspecs.core.*
import fs2.Stream
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class StatefulValidatorsSuite extends CatsEffectSuite:

  import SampleEvent.given
  import SampleEvent.{pass, reject, warn}

  test("duplicate id rejects when sendToDlq=true") {
    val rule = DuplicateIdRule("m", 10, sendToDlq = true)
    val events = Stream(pass("A", 1), pass("B", 2), pass("A", 3))
    DuplicateIdValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        results(2)._1 match
          case EngineOutcome.Reject(_, issues, _) =>
            assert(issues.exists(_.rule == "duplicate-id"))
            assert(results(2)._2.exists(_.isInstanceOf[StatefulAlert.DuplicateIdAnomaly]))
          case other => fail(s"$other")
    }
  }

  test("duplicate id warns when sendToDlq=false") {
    val rule = DuplicateIdRule("m", 10, sendToDlq = false)
    val events = Stream(pass("A", 1), pass("A", 2))
    DuplicateIdValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        results(1)._1 match
          case EngineOutcome.PassWithWarnings(_, issues) =>
            assert(issues.exists(_.rule == "duplicate-id"))
          case other => fail(s"$other")
    }
  }

  test("duplicate id falls out of window and can reappear") {
    val rule = DuplicateIdRule("m", 2, sendToDlq = true)
    val events = Stream(pass("A", 1), pass("B", 2), pass("C", 3), pass("A", 4))
    DuplicateIdValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results.forall(_._2.isEmpty), s"alerts: ${results.flatMap(_._2)}")
        assert(results.forall(_._1.isInstanceOf[EngineOutcome.Pass[?]]))
    }
  }

  test("duplicate id ignores Reject outcomes") {
    val rule = DuplicateIdRule("m", 10, sendToDlq = true)
    val events = Stream(reject(), pass("X", 1), pass("X", 2))
    DuplicateIdValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assertEquals(results.head._2, Nil)
        assert(results(2)._2.nonEmpty)
    }
  }

  test("duplicate id tracks ids from PassWithWarnings") {
    val rule = DuplicateIdRule("m", 10, sendToDlq = true)
    val events = Stream(warn("A", 1), pass("A", 2))
    DuplicateIdValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results(1)._2.exists(_.isInstanceOf[StatefulAlert.DuplicateIdAnomaly]))
    }
  }

  test("out-of-order warns or rejects based on sendToDlq") {
    val warnRule = OutOfOrderRule("m", sendToDlq = false)
    val dlqRule  = OutOfOrderRule("m", sendToDlq = true)
    val events   = Stream(pass("1", 1, 1000), pass("2", 1, 500))

    for
      warnRes <- OutOfOrderValidator.stage[SampleEvent](warnRule)(events.map(_ -> Nil)).compile.toList
      dlqRes  <- OutOfOrderValidator.stage[SampleEvent](dlqRule)(events.map(_ -> Nil)).compile.toList
    yield
      assert(warnRes(1)._1.isInstanceOf[EngineOutcome.PassWithWarnings[?]])
      assert(dlqRes(1)._1.isInstanceOf[EngineOutcome.Reject[?]])
      assert(warnRes(1)._2.exists(_.isInstanceOf[StatefulAlert.OutOfOrderAnomaly]))
  }

  test("equal timestamps are not out-of-order") {
    val rule = OutOfOrderRule("m", sendToDlq = true)
    val events = Stream(pass("1", 1, 100), pass("2", 1, 100), pass("3", 1, 200))
    OutOfOrderValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results => assert(results.forall(_._2.isEmpty))
    }
  }

  test("out-of-order does not advance watermark") {
    val rule = OutOfOrderRule("m", sendToDlq = false)
    val events = Stream(pass("1", 1, 1000), pass("2", 1, 500), pass("3", 1, 1500))
    OutOfOrderValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results(1)._2.nonEmpty)
        assertEquals(results(2)._2, Nil)
        assert(results(2)._1.isInstanceOf[EngineOutcome.Pass[?]])
    }
  }

  test("rolling average alerts below threshold when window full") {
    val rule = RollingAverageRule("m", "value", 3, 50.0)
    val events = Stream(pass("1", 100), pass("2", 10), pass("3", 5))
    RollingAverageValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results.last._2.exists {
          case StatefulAlert.RollingAverageAnomaly("value", avg, 50.0, _, 3) => avg < 50.0
          case _ => false
        })
    }
  }

  test("rolling average stays quiet above threshold") {
    val rule = RollingAverageRule("m", "value", 3, 10.0)
    val events = Stream(pass("1", 100), pass("2", 80), pass("3", 60))
    RollingAverageValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results => assert(results.forall(_._2.isEmpty))
    }
  }

  test("rolling average ignores unknown metricName") {
    val rule = RollingAverageRule("m", "temperature", 2, 50.0)
    val events = Stream(pass("1", 1), pass("2", 1))
    RollingAverageValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results => assert(results.forall(_._2.isEmpty))
    }
  }

  test("metric deviation fires on spike vs baseline") {
    val rule = MetricDeviationRule("m", "value", 3, maxDeviationPercent = 100.0)
    val events = Stream(pass("1", 10), pass("2", 10), pass("3", 10), pass("4", 50))
    MetricDeviationValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results.last._2.exists(_.isInstanceOf[StatefulAlert.MetricDeviationAnomaly]))
    }
  }

  test("metric deviation does not fire while filling window") {
    val rule = MetricDeviationRule("m", "value", 5, maxDeviationPercent = 10.0)
    val events = Stream(pass("1", 10), pass("2", 1000))
    MetricDeviationValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results => assert(results.forall(_._2.isEmpty))
    }
  }

  test("time-rolling average alerts when mean drops") {
    val rule = TimeRollingAverageRule(
      metricKey = "m",
      metricName = "value",
      windowDuration = 1.second,
      minAllowedAverage = 50.0,
      minSamples = 2,
      useEventTimestamp = true
    )
    val t0 = 1_000_000L
    val events = Stream(pass("1", 100, t0), pass("2", 10, t0 + 100), pass("3", 5, t0 + 200))
    TimeRollingAverageValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results.last._2.exists(_.isInstanceOf[StatefulAlert.TimeRollingAverageAnomaly]))
    }
  }

  test("time-rolling average respects minSamples") {
    val rule = TimeRollingAverageRule("m", "value", 1.second, 50.0, minSamples = 5, useEventTimestamp = true)
    val t0 = 2_000_000L
    val events = Stream(pass("1", 1, t0), pass("2", 1, t0 + 10))
    TimeRollingAverageValidator.stage[SampleEvent](rule)(events.map(_ -> Nil)).compile.toList.map {
      results => assert(results.forall(_._2.isEmpty))
    }
  }

  test("StatefulPipe.sequence concatenates alerts from chained stages") {
    val dup = DuplicateIdRule("dup", 10, sendToDlq = false)
    val ooo = OutOfOrderRule("ooo", sendToDlq = false)
    val pipe = StatefulPipe.sequence(
      List(DuplicateIdValidator.stage[SampleEvent](dup), OutOfOrderValidator.stage[SampleEvent](ooo))
    )
    val events = Stream(pass("A", 1, 1000), pass("B", 1, 2000), pass("A", 1, 1500))
    pipe(events).compile.toList.map { results =>
      val alerts = results(2)._2
      assert(alerts.exists(_.isInstanceOf[StatefulAlert.DuplicateIdAnomaly]))
      assert(alerts.exists(_.isInstanceOf[StatefulAlert.OutOfOrderAnomaly]))
      assertEquals(alerts.length, 2)
    }
  }

  test("StatefulPipe.sequence(Nil) is identity") {
    StatefulPipe.sequence[SampleEvent](Nil)(Stream(pass("1", 1))).compile.toList.map { results =>
      assertEquals(results.head._1, pass("1", 1))
      assertEquals(results.head._2, Nil)
    }
  }
end StatefulValidatorsSuite
