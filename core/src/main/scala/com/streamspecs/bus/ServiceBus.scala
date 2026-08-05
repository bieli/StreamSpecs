package com.streamspecs.bus

import cats.effect.IO
import com.streamspecs.engine.RoutedEvent
import fs2.Stream

/** Inbound message from a service bus (Kafka / NATS JetStream / …). */
final case class BusMessage(
    key: Option[String],
    payload: String,
    ack: IO[Unit]
)

/** Transport-agnostic consume / publish+ack API used by applications. */
trait ServiceBus[F[_]]:
  def consume: Stream[F, BusMessage]
  def publishAndAck[T](msg: BusMessage, routed: RoutedEvent[T]): F[Unit]
end ServiceBus
