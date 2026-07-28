package com.streamspecs.config

import munit.FunSuite

class CliOptionsSuite extends FunSuite:

  private def base =
    EngineConfig(
      rules = Map.empty,
      statefulRules = StatefulRules(None, None, None, None, None, None, None),
      kafka = KafkaConfig("localhost:9092", "g", "c", KafkaTopics("i", "v", "d"), "earliest"),
      metrics = MetricsConfig(
        "prometheus",
        true,
        PrometheusConfig(true, "0.0.0.0", 9464, true)
      ),
      simulationMode = true,
      defaultSendToDlq = true
    )

  test("--no-metrics-server disables scrape endpoint") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--no-metrics-server")))
    assertEquals(cfg.metrics.prometheus.enabled, false)
  }

  test("--metrics-port overrides port") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--metrics-port=9100")))
    assertEquals(cfg.metrics.prometheus.port, 9100)
  }

  test("--help detected") {
    assert(CliOptions.parse(List("--help")).help)
  }
end CliOptionsSuite
