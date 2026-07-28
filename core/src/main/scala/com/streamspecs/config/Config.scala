package com.streamspecs.config

import pureconfig.*
import pureconfig.error.ConfigReaderFailures
import scala.concurrent.duration.FiniteDuration

/** Per-rule routing / monitoring policy (matched by rule name from DataQualityValidator). */
final case class RuleAction(
    metricKey: String,
    sendToDlq: Boolean
) derives ConfigReader

final case class HeartbeatRule(
    metricKey: String,
    maxIdleDuration: FiniteDuration
) derives ConfigReader

final case class RollingAverageRule(
    metricKey: String,
    metricName: String,
    windowSizeEvents: Int,
    minAllowedAverage: Double
) derives ConfigReader

final case class TimeRollingAverageRule(
    metricKey: String,
    metricName: String,
    windowDuration: FiniteDuration,
    minAllowedAverage: Double,
    minSamples: Int,
    useEventTimestamp: Boolean
) derives ConfigReader

final case class DuplicateIdRule(
    metricKey: String,
    windowSizeEvents: Int,
    sendToDlq: Boolean
) derives ConfigReader

final case class VolumeSpikeRule(
    metricKey: String,
    windowDuration: FiniteDuration,
    maxEventsInWindow: Int
) derives ConfigReader

final case class MetricDeviationRule(
    metricKey: String,
    metricName: String,
    windowSizeEvents: Int,
    maxDeviationPercent: Double
) derives ConfigReader

final case class OutOfOrderRule(
    metricKey: String,
    sendToDlq: Boolean
) derives ConfigReader

final case class StatefulRules(
    heartbeatCheck: Option[HeartbeatRule],
    rollingAverageCheck: Option[RollingAverageRule],
    timeRollingAverageCheck: Option[TimeRollingAverageRule],
    duplicateIdCheck: Option[DuplicateIdRule],
    volumeSpikeCheck: Option[VolumeSpikeRule],
    metricDeviationCheck: Option[MetricDeviationRule],
    outOfOrderCheck: Option[OutOfOrderRule]
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

final case class MetricsConfig(
    backend: String,
    echoToConsole: Boolean,
    prometheus: PrometheusConfig
) derives ConfigReader

/** Engine configuration — domain-agnostic. Rule *logic* lives in the user's
  * [[com.streamspecs.core.DataQualityValidator]]; this config only controls routing, windows, Kafka
  * and metrics.
  */
final case class EngineConfig(
    rules: Map[String, RuleAction],
    statefulRules: StatefulRules,
    kafka: KafkaConfig,
    metrics: MetricsConfig,
    simulationMode: Boolean,
    defaultSendToDlq: Boolean
) derives ConfigReader

object AppConfig:
  def load: Either[ConfigReaderFailures, EngineConfig] =
    ConfigSource.default.at("stream-specs").load[EngineConfig]

  def loadOrThrow: EngineConfig =
    load match
      case Right(cfg) => cfg
      case Left(err) =>
        throw new IllegalStateException(s"Failed to load stream-specs config:\n$err")
