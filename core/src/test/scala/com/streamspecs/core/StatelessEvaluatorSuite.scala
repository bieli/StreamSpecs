package com.streamspecs.core

import com.streamspecs.config.*
import com.streamspecs.validation.StatelessEvaluator
import munit.FunSuite

final case class SampleEvent(id: String, value: Double, ts: Long)

object SampleEvent:
  given EventCodec[SampleEvent] with
    def decode(raw: String): Either[String, SampleEvent] =
      raw.split(",", -1).toList match
        case id :: value :: ts :: Nil =>
          value.toDoubleOption
            .flatMap(v => ts.toLongOption.map(t => SampleEvent(id, v, t)))
            .toRight("bad csv")
        case _ => Left("bad csv")
    def encode(event: SampleEvent): String =
      s"${event.id},${event.value},${event.ts}"
  end given

  given DataQualityValidator[SampleEvent] with
    def extractId(event: SampleEvent)        = Some(event.id).filter(_.nonEmpty)
    def extractTimestamp(event: SampleEvent) = Some(event.ts)
    def extractMetricValue(event: SampleEvent, metricName: String) =
      if metricName == "value" then Some(event.value) else None
    def statelessRules(event: SampleEvent) =
      Map(
        "positive-value" -> (
          if event.value > 0 then RuleVerdict.Valid
          else RuleVerdict.Invalid(s"non-positive: ${event.value}")
        ),
        "soft-id" -> (
          if event.id.startsWith("X") then RuleVerdict.Warning("id starts with X")
          else RuleVerdict.Valid
        )
      )
  end given
end SampleEvent

class StatelessEvaluatorSuite extends FunSuite:

  private val config = EngineConfig(
    rules = Map(
      "decode-failure" -> RuleAction("alerts.errors.decode_failure", true),
      "positive-value" -> RuleAction("alerts.errors.positive_value", true),
      "soft-id"        -> RuleAction("alerts.warnings.soft_id", false)
    ),
    statefulRules = StatefulRules(None, None, None, None, None, None, None),
    kafka = KafkaConfig(
      "localhost:9092",
      "g",
      "c",
      KafkaTopics("in", "ok", "dlq"),
      "earliest"
    ),
    metrics = MetricsConfig(
      "silent",
      false,
      PrometheusConfig(false, "0.0.0.0", 9464, false)
    ),
    simulationMode = true,
    defaultSendToDlq = true
  )

  private val evaluator = new StatelessEvaluator[SampleEvent](config)

  test("valid event passes") {
    evaluator.evaluate("a,10,1") match
      case EngineOutcome.Pass(e) => assertEquals(e.value, 10.0)
      case other                 => fail(s"$other")
  }

  test("hard rule goes to reject") {
    evaluator.evaluate("a,-1,1") match
      case EngineOutcome.Reject(_, issues, _) =>
        assert(issues.exists(_.rule == "positive-value"))
      case other => fail(s"$other")
  }

  test("soft warning passes with warnings") {
    evaluator.evaluate("X1,10,1") match
      case EngineOutcome.PassWithWarnings(_, issues) =>
        assert(issues.exists(_.rule == "soft-id"))
      case other => fail(s"$other")
  }

  test("decode failure rejects") {
    evaluator.evaluate("nope") match
      case EngineOutcome.Reject(_, issues, None) =>
        assert(issues.exists(_.rule == "decode-failure"))
      case other => fail(s"$other")
  }
end StatelessEvaluatorSuite
