package com.streamspecs

import com.streamspecs.config.AppConfig
import munit.FunSuite

class ConfigSuite extends FunSuite:

  test("application.conf loads via PureConfig") {
    val cfg = AppConfig.loadOrThrow
    assertEquals(cfg.simulationMode, true)
    assert(cfg.rules.contains("missing-id"))
    assertEquals(cfg.rules("invalid-email-format").sendToDlq, false)
    assert(cfg.statefulRules.heartbeatCheck.isDefined)
    assert(cfg.statefulRules.rollingPriceCheck.isDefined)
    assert(cfg.statefulRules.timeRollingPriceCheck.isDefined)
    assertEquals(cfg.statefulRules.timeRollingPriceCheck.get.minSamples, 2)
    assert(cfg.statefulRules.duplicateIdCheck.isDefined)
    assert(cfg.statefulRules.volumeSpikeCheck.isDefined)
    assert(cfg.statefulRules.priceDeviationCheck.isDefined)
    assert(cfg.statefulRules.outOfOrderCheck.isDefined)
    assert(cfg.statelessExtras.freshnessCheck.isDefined)
    assert(cfg.statelessExtras.iso4217CurrencyCheck.isDefined)
    assertEquals(cfg.statelessExtras.iso4217CurrencyCheck.get.enabled, true)
    assert(cfg.statelessExtras.allowedCurrencyCheck.isDefined)
    assertEquals(cfg.kafka.topics.incoming, "incoming-orders")
    assertEquals(cfg.metrics.backend, "prometheus")
    assertEquals(cfg.metrics.prometheus.port, 9464)
    assertEquals(cfg.metrics.prometheus.enabled, true)
  }
end ConfigSuite
