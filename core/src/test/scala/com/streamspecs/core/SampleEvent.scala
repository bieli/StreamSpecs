package com.streamspecs.core

import com.streamspecs.config.*

/** Shared sample domain for core unit tests. */
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
    def extractId(event: SampleEvent): Option[String] =
      Some(event.id).filter(_.nonEmpty)
    def extractTimestamp(event: SampleEvent): Option[Long] =
      Some(event.ts)
    def extractMetricValue(event: SampleEvent, metricName: String): Option[Double] =
      metricName match
        case "value" => Some(event.value)
        case _       => None
    def statelessRules(event: SampleEvent): Map[String, RuleVerdict] =
      Map(
        "positive-value" -> (
          if event.value > 0 then RuleVerdict.Valid
          else RuleVerdict.Invalid(s"non-positive: ${event.value}")
        ),
        "soft-id" -> (
          if event.id.startsWith("X") then RuleVerdict.Warning("id starts with X")
          else RuleVerdict.Valid
        ),
        "soft-high" -> (
          // Invalid but typically routed with sendToDlq=false in tests
          if event.value > 1000 then RuleVerdict.Invalid(s"too high: ${event.value}")
          else RuleVerdict.Valid
        )
      )
  end given

  def testConfig(
      rules: Map[String, RuleAction] = Map(
        "decode-failure" -> RuleAction("alerts.errors.decode_failure", true),
        "positive-value" -> RuleAction("alerts.errors.positive_value", true),
        "soft-id"        -> RuleAction("alerts.warnings.soft_id", false),
        "soft-high"      -> RuleAction("alerts.warnings.soft_high", false)
      ),
      defaultSendToDlq: Boolean = true
  ): EngineConfig =
    EngineConfig(
      rules = rules,
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
      defaultSendToDlq = defaultSendToDlq
    )

  def pass(id: String, value: Double, ts: Long = 1L): EngineOutcome[SampleEvent] =
    EngineOutcome.Pass(SampleEvent(id, value, ts))

  def warn(id: String, value: Double, ts: Long = 1L): EngineOutcome[SampleEvent] =
    EngineOutcome.PassWithWarnings(
      SampleEvent(id, value, ts),
      List(RuleIssue("soft-id", "id starts with X", Severity.Warning))
    )

  def reject(raw: String = "x"): EngineOutcome[SampleEvent] =
    EngineOutcome.Reject(raw, List(RuleIssue("other", "r", Severity.Error)), None)
end SampleEvent
