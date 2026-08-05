package com.streamspecs.core

import com.streamspecs.validation.StatelessEvaluator
import munit.FunSuite

class StatelessEvaluatorSuite extends FunSuite:

  import SampleEvent.given

  private val evaluator = new StatelessEvaluator[SampleEvent](SampleEvent.testConfig())

  test("valid event passes") {
    evaluator.evaluate("a,10,1") match
      case EngineOutcome.Pass(e) => assertEquals(e.value, 10.0)
      case other                 => fail(s"$other")
  }

  test("hard Invalid with sendToDlq=true rejects") {
    evaluator.evaluate("a,-1,1") match
      case EngineOutcome.Reject(_, issues, Some(e)) =>
        assert(issues.exists(_.rule == "positive-value"))
        assertEquals(e.value, -1.0)
      case other => fail(s"$other")
  }

  test("Warning with sendToDlq=false passes with warnings") {
    evaluator.evaluate("X1,10,1") match
      case EngineOutcome.PassWithWarnings(e, issues) =>
        assertEquals(e.id, "X1")
        assert(issues.exists(_.rule == "soft-id"))
        assert(issues.forall(_.severity == Severity.Warning))
      case other => fail(s"$other")
  }

  test("Invalid with sendToDlq=false is softened to PassWithWarnings") {
    // value > 1000 triggers soft-high Invalid; routing says no DLQ
    evaluator.evaluate("ok,1500,1") match
      case EngineOutcome.PassWithWarnings(_, issues) =>
        assert(issues.exists(_.rule == "soft-high"))
        assert(issues.exists(_.severity == Severity.Warning))
      case other => fail(s"$other")
  }

  test("hard Invalid wins over soft Warning - still Reject") {
    evaluator.evaluate("X1,-1,1") match
      case EngineOutcome.Reject(_, issues, _) =>
        assert(issues.exists(_.rule == "positive-value"))
        assert(issues.exists(_.rule == "soft-id"))
      case other => fail(s"$other")
  }

  test("decode failure rejects without event") {
    evaluator.evaluate("nope") match
      case EngineOutcome.Reject(raw, issues, None) =>
        assertEquals(raw, "nope")
        assert(issues.exists(_.rule == "decode-failure"))
      case other => fail(s"$other")
  }

  test("unknown rule uses defaultSendToDlq") {
    // redefine validator-less: use evaluateEvent with a custom config defaulting soft
    val softDefault = new StatelessEvaluator[SampleEvent](
      SampleEvent.testConfig(rules = Map.empty, defaultSendToDlq = false)
    )
    // positive-value not in map → defaultSendToDlq=false → PassWithWarnings
    softDefault.evaluate("a,-1,1") match
      case EngineOutcome.PassWithWarnings(_, issues) =>
        assert(issues.exists(_.rule == "positive-value"))
      case other => fail(s"$other")
  }

  test("metricKeysFor Pass / Warn / Reject") {
    val pass = evaluator.evaluate("a,10,1")
    assertEquals(evaluator.metricKeysFor(pass), List("events.valid"))

    val warn     = evaluator.evaluate("X1,10,1")
    val warnKeys = evaluator.metricKeysFor(warn)
    assert(warnKeys.contains("alerts.warnings.soft_id"))
    assert(warnKeys.contains("events.pass_with_warning"))

    val reject     = evaluator.evaluate("a,-1,1")
    val rejectKeys = evaluator.metricKeysFor(reject)
    assert(rejectKeys.contains("alerts.errors.positive_value"))
    assert(rejectKeys.contains("events.dlq"))
  }

  test("evaluateEvent skips codec and uses provided event") {
    val event = SampleEvent("a", 10, 1)
    evaluator.evaluateEvent("ignored", event) match
      case EngineOutcome.Pass(e) => assertEquals(e, event)
      case other                 => fail(s"$other")
  }
end StatelessEvaluatorSuite
