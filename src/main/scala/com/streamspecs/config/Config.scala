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

final case class StatefulRules(
    heartbeatCheck: Option[HeartbeatRule],
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

object AppConfig:
  def load: Either[ConfigReaderFailures, StreamValidatorConfig] =
    ConfigSource.default.at("stream-validator").load[StreamValidatorConfig]

  def loadOrThrow: StreamValidatorConfig =
    load match
      case Right(cfg) => cfg
      case Left(err) =>
        throw new IllegalStateException(s"Failed to load stream-validator config:\n$err")
