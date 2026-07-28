package com.streamspecs.core

import com.streamspecs.config.*
import com.streamspecs.validation.{DuplicateIdValidator, RollingAverageValidator}
import fs2.Stream
import munit.CatsEffectSuite

class GenericStatefulSuite extends CatsEffectSuite:

  import SampleEvent.given

  private val dupRule = DuplicateIdRule("alerts.stateful.duplicate_id", 10, sendToDlq = true)
  private val rollRule =
    RollingAverageRule("alerts.stateful.low_rolling_average", "value", 3, minAllowedAverage = 50.0)

  private def pass(id: String, value: Double, ts: Long = 1L) =
    EngineOutcome.Pass(SampleEvent(id, value, ts))

  test("duplicate id rejects when configured") {
    val events = Stream(pass("A", 1), pass("B", 2), pass("A", 3))
    DuplicateIdValidator.stage[SampleEvent](dupRule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        results(2)._1 match
          case EngineOutcome.Reject(_, issues, _) =>
            assert(issues.exists(_.rule == "duplicate-id"))
          case other => fail(s"$other")
    }
  }

  test("rolling average uses extractMetricValue") {
    val events = Stream(pass("1", 100), pass("2", 10), pass("3", 5))
    RollingAverageValidator.stage[SampleEvent](rollRule)(events.map(_ -> Nil)).compile.toList.map {
      results =>
        assert(results.last._2.exists(_.isInstanceOf[StatefulAlert.RollingAverageAnomaly]))
    }
  }
end GenericStatefulSuite
