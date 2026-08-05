package com.streamspecs.metrics

import munit.CatsEffectSuite
import scala.jdk.CollectionConverters.*

class PrometheusMetricsSuite extends CatsEffectSuite:

  test("maps event results and rule names to labeled counters") {
    val registry = PrometheusRegistry.create(jvmMetrics = false)
    for
      metrics <- Metrics.prometheus(registry)
      _       <- metrics.increment("events.valid")
      _       <- metrics.increment("alerts.errors.temperature_bound")
      _       <- metrics.increment("alerts.stateful.duplicate_id")
    yield
      val names = registry.collectorRegistry.metricFamilySamples().asScala.map(_.name).toSet
      assert(names.contains("streamspecs_events"))
      assert(names.contains("streamspecs_rule_violations"))
      assert(names.contains("streamspecs_stateful_alerts"))
    end for
  }
end PrometheusMetricsSuite
