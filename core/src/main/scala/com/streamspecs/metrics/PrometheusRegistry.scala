package com.streamspecs.metrics

import io.prometheus.client.{CollectorRegistry, Counter}
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports

import java.net.InetSocketAddress
import scala.collection.concurrent.TrieMap

final class PrometheusRegistry(val collectorRegistry: CollectorRegistry):

  private val events: Counter =
    Counter
      .build()
      .name("streamspecs_events_total")
      .help("Processed events by routing result")
      .labelNames("result")
      .register(collectorRegistry)

  private val ruleViolations: Counter =
    Counter
      .build()
      .name("streamspecs_rule_violations_total")
      .help("Rule violations from user DataQualityValidator")
      .labelNames("category", "rule")
      .register(collectorRegistry)

  private val statefulAlerts: Counter =
    Counter
      .build()
      .name("streamspecs_stateful_alerts_total")
      .help("Stateful / windowed data-quality alerts")
      .labelNames("alert_type")
      .register(collectorRegistry)

  private val dynamic = TrieMap.empty[String, Counter]

  def inc(key: String): Unit =
    key match
      case "events.valid"             => events.labels("valid").inc()
      case "events.dlq"               => events.labels("dlq").inc()
      case "events.pass_with_warning" => events.labels("pass_with_warning").inc()
      case k if k.startsWith("alerts.errors.") =>
        ruleViolations.labels("errors", sanitizeLabel(k.stripPrefix("alerts.errors."))).inc()
      case k if k.startsWith("alerts.warnings.") =>
        ruleViolations.labels("warnings", sanitizeLabel(k.stripPrefix("alerts.warnings."))).inc()
      case k if k.startsWith("alerts.stateful.") =>
        statefulAlerts.labels(sanitizeLabel(k.stripPrefix("alerts.stateful."))).inc()
      case other =>
        val name = sanitizeMetricName(s"streamspecs_${other.replace('.', '_')}")
        dynamic
          .getOrElseUpdate(
            name,
            Counter
              .build()
              .name(name)
              .help(s"Auto-registered counter for key '$other'")
              .register(collectorRegistry)
          )
          .inc()

  private def sanitizeLabel(raw: String): String =
    raw.replace('-', '_').replace('.', '_').toLowerCase

  private def sanitizeMetricName(raw: String): String =
    raw.replaceAll("[^a-zA-Z0-9_:]", "_").toLowerCase
end PrometheusRegistry

object PrometheusRegistry:
  def create(jvmMetrics: Boolean): PrometheusRegistry =
    val registry = new CollectorRegistry(true)
    if jvmMetrics then DefaultExports.register(registry)
    new PrometheusRegistry(registry)

  def startHttpServer(registry: PrometheusRegistry, host: String, port: Int): HTTPServer =
    new HTTPServer(new InetSocketAddress(host, port), registry.collectorRegistry, true)
end PrometheusRegistry
