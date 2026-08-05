package com.streamspecs.bus

import cats.effect.{IO, Resource}
import com.streamspecs.config.MessagingConfig

object ServiceBuses:

  /** Build a [[ServiceBus]] for the configured messaging backend (`kafka` or `nats`). */
  def resource(messaging: MessagingConfig): Resource[IO, ServiceBus[IO]] =
    messaging.backend.trim.toLowerCase match
      case "nats" | "nats-jetstream" | "jetstream" =>
        NatsJetStreamBus.resource(messaging)
      case "kafka" =>
        KafkaServiceBus.resource(messaging)
      case other =>
        Resource.raiseError[IO, ServiceBus[IO], Throwable](
          new IllegalArgumentException(
            s"Unknown messaging.backend '$other' (use: kafka | nats)"
          )
        )
end ServiceBuses
