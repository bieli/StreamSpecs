package com.streamspecs.bus

import cats.effect.IO
import com.streamspecs.config.*
import munit.CatsEffectSuite

class ServiceBusesSuite extends CatsEffectSuite:

  private val messaging = MessagingConfig(
    backend = "unknown-bus",
    destinations = Destinations("in", "ok", "dlq"),
    kafka = KafkaConfig("localhost:9092", "g", "c", "earliest"),
    nats = NatsConfig("nats://localhost:4222", "S", "d", true, "all")
  )

  test("unknown messaging.backend fails to allocate") {
    ServiceBuses.resource(messaging).use(_ => IO.unit).attempt.map {
      case Left(err: IllegalArgumentException) =>
        assert(err.getMessage.contains("Unknown messaging.backend"))
      case other => fail(s"expected IllegalArgumentException, got $other")
    }
  }
end ServiceBusesSuite
