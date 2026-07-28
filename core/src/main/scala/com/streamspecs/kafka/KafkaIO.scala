package com.streamspecs.kafka

import cats.effect.{IO, Resource}
import com.streamspecs.config.KafkaConfig
import com.streamspecs.engine.RoutedEvent
import fs2.Stream
import fs2.kafka.*

object KafkaIO:

  def consumerSettings(cfg: KafkaConfig): ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(cfg.groupId)
      .withClientId(cfg.clientId)
      .withAutoOffsetReset(
        if cfg.autoOffsetReset.equalsIgnoreCase("earliest") then AutoOffsetReset.Earliest
        else AutoOffsetReset.Latest
      )
      .withEnableAutoCommit(false)

  def producerSettings(cfg: KafkaConfig): ProducerSettings[IO, String, String] =
    ProducerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withClientId(s"${cfg.clientId}-producer")

  def consume(cfg: KafkaConfig): Stream[IO, CommittableConsumerRecord[IO, String, String]] =
    KafkaConsumer.stream(consumerSettings(cfg)).subscribeTo(cfg.topics.incoming).records

  def produceAndCommit[T](
      producer: KafkaProducer[IO, String, String],
      key: Option[String],
      routed: RoutedEvent[T],
      offset: CommittableOffset[IO]
  ): IO[Unit] =
    val record = ProducerRecord(routed.targetTopic, key.getOrElse(""), routed.payload)
    producer.produce(ProducerRecords.one(record)).flatten.void *> offset.commit

  def producerResource(cfg: KafkaConfig): Resource[IO, KafkaProducer[IO, String, String]] =
    KafkaProducer.resource(producerSettings(cfg))
end KafkaIO
