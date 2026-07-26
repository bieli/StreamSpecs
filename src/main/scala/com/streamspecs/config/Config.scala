package com.streamspecs.config

import pureconfig.*
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.duration.FiniteDuration

/** Per-rule monitoring + DLQ routing for a named error code. */
final case class RuleAction(
    metricKey: String,
    sendToDlq: Boolean
) derives ConfigReader

/** Dead Man's Switch: alert when no events arrive within maxIdle. */
final case class HeartbeatRule(
    metricKey: String,
    maxIdleDuration: FiniteDuration
) derives ConfigReader

/** Count-based rolling average anomaly on a numeric field. */
final case class RollingAverageRule(
    metricKey: String,
    field: String,
    windowSizeEvents: Int,
    minAllowedAverage: Double
) derives ConfigReader

/** Time-based rolling average over the last `windowDuration`. */
final case class TimeRollingAverageRule(
    metricKey: String,
    field: String,
    windowDuration: FiniteDuration,
    minAllowedAverage: Double,
    minSamples: Int,
    useEventTimestamp: Boolean
) derives ConfigReader

/** Detect duplicate event IDs within the last N seen ids. */
final case class DuplicateIdRule(
    metricKey: String,
    windowSizeEvents: Int,
    sendToDlq: Boolean
) derives ConfigReader

/** Alert when too many events arrive inside a sliding time window. */
final case class VolumeSpikeRule(
    metricKey: String,
    windowDuration: FiniteDuration,
    maxEventsInWindow: Int
) derives ConfigReader

/** Alert when |price - rollingAvg| / rollingAvg exceeds max deviation %. */
final case class PriceDeviationRule(
    metricKey: String,
    windowSizeEvents: Int,
    maxDeviationPercent: Double
) derives ConfigReader

/** Alert when eventTimestamp goes backwards vs last seen. */
final case class OutOfOrderRule(
    metricKey: String,
    sendToDlq: Boolean
) derives ConfigReader

/** Stateless: eventTimestamp lag vs wall clock. */
final case class FreshnessRule(
    metricKey: String,
    maxLag: FiniteDuration,
    sendToDlq: Boolean
) derives ConfigReader

/** Stateless: validate currency against ISO 4217 (3-letter alphabetic codes). Enabled by default
  * for banking / insurance payloads.
  *
  * @param enabled
  *   when false, skip ISO validation entirely
  * @param requireField
  *   when true, missing `currency` is an error; when false, only validate the field if it is
  *   present
  */
final case class Iso4217CurrencyRule(
    enabled: Boolean,
    metricKey: String,
    sendToDlq: Boolean,
    requireField: Boolean
) derives ConfigReader

object Iso4217CurrencyRule:
  /** Default banking/insurance policy: ISO 4217 on when currency is present. */
  val default: Iso4217CurrencyRule =
    Iso4217CurrencyRule(
      enabled = true,
      metricKey = "alerts.errors.invalid_iso4217_currency",
      sendToDlq = true,
      requireField = false
    )

/** Optional extra: further restrict to a business allow-list (subset of ISO 4217). */
final case class AllowedCurrencyRule(
    metricKey: String,
    allowed: List[String],
    sendToDlq: Boolean
) derives ConfigReader

final case class StatefulRules(
    heartbeatCheck: Option[HeartbeatRule],
    rollingPriceCheck: Option[RollingAverageRule],
    timeRollingPriceCheck: Option[TimeRollingAverageRule],
    duplicateIdCheck: Option[DuplicateIdRule],
    volumeSpikeCheck: Option[VolumeSpikeRule],
    priceDeviationCheck: Option[PriceDeviationRule],
    outOfOrderCheck: Option[OutOfOrderRule]
) derives ConfigReader

final case class StatelessExtras(
    freshnessCheck: Option[FreshnessRule],
    iso4217CurrencyCheck: Option[Iso4217CurrencyRule],
    allowedCurrencyCheck: Option[AllowedCurrencyRule]
) derives ConfigReader

final case class KafkaTopics(
    incoming: String,
    valid: String,
    dlq: String
) derives ConfigReader

final case class KafkaConfig(
    bootstrapServers: String,
    groupId: String,
    clientId: String,
    topics: KafkaTopics,
    autoOffsetReset: String
) derives ConfigReader

final case class PrometheusConfig(
    enabled: Boolean,
    host: String,
    port: Int,
    jvmMetrics: Boolean
) derives ConfigReader

/** Metrics backend: console | silent | prometheus */
final case class MetricsConfig(
    backend: String,
    echoToConsole: Boolean,
    prometheus: PrometheusConfig
) derives ConfigReader

final case class StreamValidatorConfig(
    rules: Map[String, RuleAction],
    statefulRules: StatefulRules,
    statelessExtras: StatelessExtras,
    kafka: KafkaConfig,
    metrics: MetricsConfig,
    simulationMode: Boolean
) derives ConfigReader

object AppConfig:
  def load: Either[ConfigReaderFailures, StreamValidatorConfig] =
    ConfigSource.default.at("stream-validator").load[StreamValidatorConfig]

  def loadOrThrow: StreamValidatorConfig =
    load match
      case Right(cfg) => cfg
      case Left(err) =>
        throw new IllegalStateException(s"Failed to load stream-validator config:\n$err")
