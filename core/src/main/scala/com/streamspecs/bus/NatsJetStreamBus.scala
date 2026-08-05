package com.streamspecs.bus

import cats.effect.{IO, Resource}
import com.streamspecs.config.{Destinations, MessagingConfig, NatsConfig}
import com.streamspecs.engine.RoutedEvent
import fs2.Stream
import io.nats.client.*
import io.nats.client.api.{DeliverPolicy, RetentionPolicy, StorageType, StreamConfiguration}
import io.nats.client.impl.Headers

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*

/** NATS JetStream implementation of [[ServiceBus]].
  *
  * Destinations are JetStream subjects. A single stream retains `incoming`, `valid`, and `dlq`.
  * Consumption uses a durable pull consumer on `destinations.incoming`.
  */
final class NatsJetStreamBus private (
    js: JetStream,
    subscription: JetStreamSubscription,
    closed: AtomicBoolean
) extends ServiceBus[IO]:

  def consume: Stream[IO, BusMessage] =
    Stream.repeatEval(pollOne).unNone

  private def pollOne: IO[Option[BusMessage]] =
    IO.blocking {
      if closed.get() then None
      else
        subscription.pull(1)
        Option(subscription.nextMessage(Duration.ofMillis(500))).map { msg =>
          BusMessage(
            key = headerValue(msg, NatsJetStreamBus.KeyHeader),
            payload = new String(msg.getData, StandardCharsets.UTF_8),
            ack = IO.blocking(msg.ack()).void
          )
        }
    }

  def publishAndAck[T](msg: BusMessage, routed: RoutedEvent[T]): IO[Unit] =
    IO.blocking {
      val headers = new Headers()
      msg.key.foreach(k => headers.put(NatsJetStreamBus.KeyHeader, k))
      js.publish(
        routed.targetTopic,
        headers,
        routed.payload.getBytes(StandardCharsets.UTF_8)
      )
    }.void *> msg.ack

  private def headerValue(msg: Message, name: String): Option[String] =
    Option(msg.getHeaders)
      .flatMap(h => Option(h.get(name)))
      .flatMap(_.asScala.headOption)
      .filter(_.nonEmpty)
end NatsJetStreamBus

object NatsJetStreamBus:

  val KeyHeader: String = "SS-Key"

  def resource(messaging: MessagingConfig): Resource[IO, ServiceBus[IO]] =
    val cfg  = messaging.nats
    val dest = messaging.destinations
    Resource
      .make(connect(cfg, dest)) { case (nc, _, _, closed) =>
        IO.blocking {
          closed.set(true)
          nc.close()
        }.void
      }
      .map { case (_, js, sub, closed) =>
        NatsJetStreamBus(js, sub, closed)
      }
  end resource

  private def connect(
      cfg: NatsConfig,
      dest: Destinations
  ): IO[(Connection, JetStream, JetStreamSubscription, AtomicBoolean)] =
    IO.blocking {
      val builder = new Options.Builder().connectionName(cfg.durable)
      cfg.servers
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .foreach(builder.server)
      val nc     = Nats.connect(builder.build())
      val jsm    = nc.jetStreamManagement()
      val js     = nc.jetStream()
      val closed = new AtomicBoolean(false)

      if cfg.createStreamIfMissing then ensureStream(jsm, cfg.stream, dest)

      val deliver =
        if cfg.deliverPolicy.equalsIgnoreCase("new") then DeliverPolicy.New
        else DeliverPolicy.All

      val pullOpts = PullSubscribeOptions
        .builder()
        .stream(cfg.stream)
        .durable(cfg.durable)
        .configuration(
          io.nats.client.api.ConsumerConfiguration
            .builder()
            .deliverPolicy(deliver)
            .ackPolicy(io.nats.client.api.AckPolicy.Explicit)
            .build()
        )
        .build()

      val sub = js.subscribe(dest.incoming, pullOpts)
      (nc, js, sub, closed)
    }

  private def ensureStream(
      jsm: JetStreamManagement,
      stream: String,
      dest: Destinations
  ): Unit =
    val subjects = List(dest.incoming, dest.valid, dest.dlq).distinct
    try
      val info = jsm.getStreamInfo(stream)
      val have = info.getConfiguration.getSubjects.asScala.toSet
      if !subjects.forall(have.contains) then
        val merged = (have ++ subjects).toList
        jsm.updateStream(
          StreamConfiguration
            .builder(info.getConfiguration)
            .subjects(merged.asJava)
            .build()
        )
    catch
      case _: JetStreamApiException =>
        jsm.addStream(
          StreamConfiguration
            .builder()
            .name(stream)
            .subjects(subjects.asJava)
            .storageType(StorageType.File)
            .retentionPolicy(RetentionPolicy.Limits)
            .build()
        )
    end try
  end ensureStream
end NatsJetStreamBus
