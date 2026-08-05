package com.streamspecs.bus

import cats.effect.{IO, Resource}
import com.streamspecs.config.MessagingConfig
import com.streamspecs.engine.RoutedEvent
import com.streamspecs.kafka.KafkaIO
import fs2.Stream
import fs2.kafka.KafkaProducer

/** Kafka implementation of [[ServiceBus]]. */
final class KafkaServiceBus private (
    messaging: MessagingConfig,
    producer: KafkaProducer[IO, String, String]
) extends ServiceBus[IO]:

  private val destinations = messaging.destinations

  def consume: Stream[IO, BusMessage] =
    KafkaIO.consume(messaging.kafka, destinations.incoming).map { cr =>
      BusMessage(
        key = Option(cr.record.key).filter(_.nonEmpty),
        payload = cr.record.value,
        ack = cr.offset.commit
      )
    }

  def publishAndAck[T](msg: BusMessage, routed: RoutedEvent[T]): IO[Unit] =
    KafkaIO.produceAndCommit(producer, msg.key, routed, msg.ack)
end KafkaServiceBus

object KafkaServiceBus:
  def resource(messaging: MessagingConfig): Resource[IO, ServiceBus[IO]] =
    KafkaIO.producerResource(messaging.kafka).map(producer => KafkaServiceBus(messaging, producer))
end KafkaServiceBus
