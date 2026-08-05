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

  test("-h is help") {
    assert(CliOptions.parse(List("-h")).help)
  }

  test("--metrics-server enables scrape endpoint") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--metrics-server")))
    assertEquals(cfg.metrics.prometheus.enabled, true)
  }

  test("--metrics-server=false disables") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--metrics-server=false")))
    assertEquals(cfg.metrics.prometheus.enabled, false)
  }

  test("--metrics-host overrides bind address") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--metrics-host", "127.0.0.1")))
    assertEquals(cfg.metrics.prometheus.host, "127.0.0.1")
  }

  test("--metrics-backend accepts known backends") {
    val cfg = CliOptions.applyTo(base, CliOptions.parse(List("--metrics-backend=silent")))
    assertEquals(cfg.metrics.backend, "silent")
  }

  test("invalid --metrics-port records error") {
    val cli = CliOptions.parse(List("--metrics-port", "99999"))
    assert(!cli.isValid)
    assert(cli.errors.exists(_.contains("Invalid --metrics-port")))
  }

  test("invalid --metrics-backend records error") {
    val cli = CliOptions.parse(List("--metrics-backend", "graphite"))
    assert(!cli.isValid)
  }

  test("unknown argument records error") {
    val cli = CliOptions.parse(List("--wat"))
    assert(cli.errors.exists(_.contains("Unknown argument")))
  }

  test("invalid boolean for metrics-server records error") {
    val cli = CliOptions.parse(List("--metrics-server=maybe"))
    assert(!cli.isValid)
  }
end CliOptionsSuite
