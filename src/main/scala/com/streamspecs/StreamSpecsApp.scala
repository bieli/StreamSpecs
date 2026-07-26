package com.streamspecs

import cats.effect.{ExitCode, IO, IOApp, Ref}
import com.streamspecs.config.{AppConfig, CliOptions, MetricsConfig, StreamValidatorConfig}
import com.streamspecs.domain.StatefulAlert
import com.streamspecs.kafka.KafkaIO
import com.streamspecs.metrics.{Metrics, PrometheusServer}
import com.streamspecs.pipeline.ValidationPipeline
import com.streamspecs.pipeline.ValidationPipeline.RoutedEvent
import com.streamspecs.validation.{StreamHeartbeat, VolumeSpikeDetector}
import fs2.Stream

import scala.concurrent.duration.*

/** StreamSpecs - real-time streaming data-quality validator.
  *
  * Metrics HTTP scrape server is controlled by (highest wins):
  *   1. CLI: `--metrics-server` / `--no-metrics-server` / `--metrics-port` 2. ENV:
  *      `STREAMSPECS_METRICS_SERVER`, `STREAMSPECS_METRICS_PORT`, … 3. HOCON:
  *      `stream-validator.metrics.prometheus.enabled`
  *
  * Default scrape endpoint: http://localhost:9464/metrics
  */
object StreamSpecsApp extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val cli = CliOptions.parse(args)
    if cli.help then IO.println(CliOptions.helpText).as(ExitCode.Success)
    else if !cli.isValid then
      IO.println(cli.errors.mkString("\n")) *>
        IO.println("\nUse --help for usage.") *>
        IO.pure(ExitCode.Error)
    else
      val config = CliOptions.applyTo(AppConfig.loadOrThrow, cli)
      runApp(config).as(ExitCode.Success)
  end run

  private def runApp(config: StreamValidatorConfig): IO[Unit] =
    PrometheusServer.resource(config.metrics).use { case (registry, httpServer) =>
      for
        _ <- IO.println("StreamSpecs validator starting...")
        _ <- IO.println(s"  simulation-mode = ${config.simulationMode}")
        _ <- IO.println(s"  incoming topic  = ${config.kafka.topics.incoming}")
        _ <- IO.println(s"  metrics backend = ${config.metrics.backend}")
        _ <- httpServer match
          case Some(_) =>
            IO.println(
              s"  metrics server  = ON  http://${bindHost(config)}:${config.metrics.prometheus.port}/metrics"
            )
          case None =>
            IO.println("  metrics server  = OFF  (counters may still be recorded in-process)")
        metrics     <- makeMetrics(config.metrics, registry)
        now         <- IO.realTime.map(_.toMillis)
        heartbeat   <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(now))
        volumeState <- Ref.of[IO, VolumeSpikeDetector.State](VolumeSpikeDetector.State.empty)
        (transform, watchdog) = ValidationPipeline.build(config, metrics, heartbeat, volumeState)
        _ <-
          if config.simulationMode then runSimulation(transform, watchdog)
          else runKafka(config, transform, watchdog)
        snap <- metrics.snapshot
        _    <- IO.println(s"Final metrics snapshot: $snap")
        _ <-
          if config.metrics.prometheus.enabled && config.simulationMode then
            IO.println(
              s"Metrics server still up for 5s - curl http://127.0.0.1:${config.metrics.prometheus.port}/metrics"
            ) *> IO.sleep(5.seconds)
          else IO.unit
      yield ()
    }

  private def bindHost(config: StreamValidatorConfig): String =
    val h = config.metrics.prometheus.host
    if h == "0.0.0.0" then "127.0.0.1" else h

  private def makeMetrics(
      cfg: MetricsConfig,
      registry: com.streamspecs.metrics.PrometheusRegistry
  ): IO[Metrics[IO]] =
    cfg.backend.toLowerCase match
      case "prometheus" => Metrics.prometheus(registry, echoToConsole = cfg.echoToConsole)
      case "silent"     => Metrics.silent
      case _            => Metrics.console

  private def runSimulation(
      transform: Stream[IO, String] => Stream[IO, RoutedEvent],
      watchdog: Stream[IO, StatefulAlert]
  ): IO[Unit] =
    val now = System.currentTimeMillis()
    val samples = Stream(
      s"""{"id":"ORD-001","price":99.99,"email":"ok@example.com","currency":"PLN","eventTimestamp":$now}""",
      s"""{"id":"ORD-002","price":-5.0,"email":"hacker@evil.com","currency":"PLN","eventTimestamp":$now}""",
      s"""{"id":"ORD-003","price":40.0,"email":"not-an-email","currency":"EUR","eventTimestamp":${now + 1}}""",
      s"""{"id":"ORD-004","price":55.0,"email":"alice@example.com","currency":"USD","eventTimestamp":${now + 2}}""",
      s"""{"id":"ORD-005","price":12.0,"email":"bob@example.com","currency":"PLN","eventTimestamp":${now + 3}}""",
      s"""{"id":"ORD-001","price":99.99,"email":"ok@example.com","currency":"PLN","eventTimestamp":${now + 4}}""",
      s"""{"id":"ORD-006","price":70.0,"email":"fx@example.com","currency":"BTC","eventTimestamp":${now + 5}}""",
      s"""{"id":"ORD-006b","price":70.0,"email":"fx2@example.com","currency":"EU","eventTimestamp":${now + 6}}""",
      s"""{"id":"ORD-007","price":60.0,"email":"old@example.com","currency":"PLN","eventTimestamp":${now - 600_000}}""",
      s"""{"id":"ORD-008","price":65.0,"email":"ooo@example.com","currency":"PLN","eventTimestamp":${now - 10}}""",
      s"""{"id":"ORD-009","price":500.0,"email":"spike@example.com","currency":"PLN","eventTimestamp":${now + 10}}""",
      s"""{"id":"","price":50.0,"email":"noid@example.com","currency":"PLN","eventTimestamp":${now + 11}}""",
      """{not-json"""
    )

    val incoming =
      samples.metered[IO](300.millis) ++
        Stream.sleep[IO](6.seconds).drain ++
        Stream.emit(
          s"""{"id":"ORD-999","price":80.0,"email":"back@example.com","currency":"PLN","eventTimestamp":${System
              .currentTimeMillis()}}"""
        )

    val data =
      transform(incoming).evalMap { routed =>
        IO.println(s"  -> topic=${routed.targetTopic}  payload=${routed.payload.take(120)}")
      }

    IO.println("Running simulation mode (no Kafka broker required)...") *>
      data.mergeHaltBoth(watchdog.drain).interruptAfter(14.seconds).compile.drain
  end runSimulation

  private def runKafka(
      config: StreamValidatorConfig,
      transform: Stream[IO, String] => Stream[IO, RoutedEvent],
      watchdog: Stream[IO, StatefulAlert]
  ): IO[Unit] =
    KafkaIO.producerResource(config.kafka).use { producer =>
      val processed =
        KafkaIO.consume(config.kafka).evalMap { cr =>
          val raw = cr.record.value
          val key = Option(cr.record.key)
          transform(Stream.emit(raw)).compile.toList.flatMap {
            case routed :: _ => KafkaIO.produceAndCommit(producer, key, routed, cr.offset)
            case Nil         => cr.offset.commit
          }
        }

      IO.println(
        s"Consuming from ${config.kafka.topics.incoming} @ ${config.kafka.bootstrapServers}"
      ) *>
        processed.mergeHaltBoth(watchdog.drain).compile.drain
    }
end StreamSpecsApp
