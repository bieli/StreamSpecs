package com.streamspecs

import com.streamspecs.metrics.{Metrics, PrometheusRegistry}
import munit.CatsEffectSuite

import scala.jdk.CollectionConverters.*

class PrometheusMetricsSuite extends CatsEffectSuite:

  test("prometheus registry maps event results to labeled counter") {
    val registry = PrometheusRegistry.create(jvmMetrics = false)
    for
      metrics <- Metrics.prometheus(registry, echoToConsole = false)
      _       <- metrics.increment("events.valid")
      _       <- metrics.increment("events.valid")
      _       <- metrics.increment("events.dlq")
      snap    <- metrics.snapshot
    yield
      assertEquals(snap.get("events.valid"), Some(2L))
      assertEquals(snap.get("events.dlq"), Some(1L))
      val samples      = registry.collectorRegistry.metricFamilySamples().asScala.toList
      val eventsFamily = samples.find(_.name == "streamspecs_events")
      assert(eventsFamily.isDefined, s"missing streamspecs_events family in $samples")
    end for
  }

  test("rule violations and stateful alerts map to labeled counters") {
    val registry = PrometheusRegistry.create(jvmMetrics = false)
    for
      metrics <- Metrics.prometheus(registry)
      _       <- metrics.increment("alerts.errors.missing_id")
      _       <- metrics.increment("alerts.warnings.invalid_email")
      _       <- metrics.increment("alerts.stateful.duplicate_id")
      _       <- metrics.increment("alerts.stateful.temporal")
    yield
      val families = registry.collectorRegistry.metricFamilySamples().asScala.map(_.name).toSet
      assert(families.contains("streamspecs_rule_violations"))
      assert(families.contains("streamspecs_stateful_alerts"))
    end for
  }

  test("unknown keys get a dynamic sanitized counter") {
    val registry = PrometheusRegistry.create(jvmMetrics = false)
    for
      metrics <- Metrics.prometheus(registry)
      _       <- metrics.increment("custom.metric.foo")
    yield
      val names = registry.collectorRegistry.metricFamilySamples().asScala.map(_.name).toSet
      assert(names.exists(_.contains("custom_metric_foo")), s"names=$names")
  }
end PrometheusMetricsSuite
