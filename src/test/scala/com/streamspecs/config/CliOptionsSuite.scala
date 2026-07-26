package com.streamspecs

import com.streamspecs.config.{
  AppConfig,
  CliOptions,
  MetricsConfig,
  PrometheusConfig,
  StreamValidatorConfig
}
import munit.FunSuite

class CliOptionsSuite extends FunSuite:

  private def baseMetrics =
    MetricsConfig(
      backend = "prometheus",
      echoToConsole = true,
      prometheus = PrometheusConfig(
        enabled = true,
        host = "0.0.0.0",
        port = 9464,
        jvmMetrics = true
      )
    )

  private def baseConfig =
    AppConfig.loadOrThrow.copy(metrics = baseMetrics)

  test("--no-metrics-server disables scrape endpoint") {
    val cli = CliOptions.parse(List("--no-metrics-server"))
    assertEquals(cli.metricsServer, Some(false))
    val cfg = CliOptions.applyTo(baseConfig, cli)
    assertEquals(cfg.metrics.prometheus.enabled, false)
  }

  test("--metrics-server enables scrape endpoint") {
    val cfg = CliOptions.applyTo(
      baseConfig.copy(metrics =
        baseMetrics.copy(prometheus = baseMetrics.prometheus.copy(enabled = false))
      ),
      CliOptions.parse(List("--metrics-server"))
    )
    assertEquals(cfg.metrics.prometheus.enabled, true)
  }

  test("--metrics-server=false and --metrics-port") {
    val cli = CliOptions.parse(List("--metrics-server=false", "--metrics-port", "9100"))
    assertEquals(cli.metricsServer, Some(false))
    assertEquals(cli.metricsPort, Some(9100))
    val cfg = CliOptions.applyTo(baseConfig, cli)
    assertEquals(cfg.metrics.prometheus.enabled, false)
    assertEquals(cfg.metrics.prometheus.port, 9100)
  }

  test("--metrics-backend and --metrics-host") {
    val cli = CliOptions.parse(List("--metrics-backend", "silent", "--metrics-host", "127.0.0.1"))
    val cfg = CliOptions.applyTo(baseConfig, cli)
    assertEquals(cfg.metrics.backend, "silent")
    assertEquals(cfg.metrics.prometheus.host, "127.0.0.1")
  }

  test("invalid backend is reported") {
    val cli = CliOptions.parse(List("--metrics-backend", "graphite"))
    assert(!cli.isValid)
    assert(cli.errors.exists(_.contains("Invalid --metrics-backend")))
  }

  test("unknown flag is reported") {
    val cli = CliOptions.parse(List("--wat"))
    assert(!cli.isValid)
  }

  test("--help is detected") {
    assert(CliOptions.parse(List("--help")).help)
    assert(CliOptions.parse(List("-h")).help)
  }

  test("empty args keep config defaults") {
    val cfg = CliOptions.applyTo(baseConfig, CliOptions.parse(Nil))
    assertEquals(cfg.metrics.prometheus.enabled, true)
    assertEquals(cfg.metrics.prometheus.port, 9464)
  }
end CliOptionsSuite
