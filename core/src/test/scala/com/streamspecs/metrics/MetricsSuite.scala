package com.streamspecs.metrics

import munit.CatsEffectSuite

class MetricsSuite extends CatsEffectSuite:

  test("silent metrics accumulate snapshot counters") {
    for
      m <- Metrics.silent
      _ <- m.increment("events.valid")
      _ <- m.increment("events.valid")
      _ <- m.increment("events.dlq")
      snap <- m.snapshot
    yield
      assertEquals(snap.get("events.valid"), Some(2L))
      assertEquals(snap.get("events.dlq"), Some(1L))
  }

  test("prometheus metrics keep local snapshot in sync") {
    val registry = PrometheusRegistry.create(jvmMetrics = false)
    for
      m <- Metrics.prometheus(registry)
      _ <- m.increment("events.valid")
      _ <- m.increment("alerts.warnings.soft_id")
      snap <- m.snapshot
    yield
      assertEquals(snap.get("events.valid"), Some(1L))
      assertEquals(snap.get("alerts.warnings.soft_id"), Some(1L))
  }
end MetricsSuite
