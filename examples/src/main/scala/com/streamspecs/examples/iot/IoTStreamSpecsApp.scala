package com.streamspecs.examples.iot

import cats.effect.{ExitCode, IO, IOApp, Ref}
import com.streamspecs.bus.ServiceBuses
import com.streamspecs.config.{AppConfig, CliOptions, EngineConfig, MetricsConfig}
import com.streamspecs.core.StatefulAlert
import com.streamspecs.engine.{RoutedEvent, ValidationEngine}
import com.streamspecs.metrics.{Metrics, PrometheusRegistry, PrometheusServer}
import com.streamspecs.validation.{StreamHeartbeat, VolumeSpikeDetector}
import fs2.Stream

import scala.concurrent.duration.*

/** Example application: StreamSpecs engine + IoT TemperatureSensorEvent domain.
  *
  * Demonstrates that the library has no dependency on finance fields (price/currency).
  */
object IoTStreamSpecsApp extends IOApp:

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

  private def runApp(config: EngineConfig): IO[Unit] =
    PrometheusServer.resource(config.metrics).use { case (registry, httpServer) =>
      for
        _ <- IO.println("StreamSpecs IoT example starting...")
        _ <- IO.println(s"  simulation-mode    = ${config.simulationMode}")
        _ <- IO.println(s"  messaging.backend  = ${config.messaging.backend}")
        _ <- IO.println(s"  domain             = TemperatureSensorEvent")
        _ <- IO.println(s"  metrics backend    = ${config.metrics.backend}")
        _ <- httpServer match
          case Some(_) =>
            IO.println(
              s"  metrics server     = ON  http://127.0.0.1:${config.metrics.prometheus.port}/metrics"
            )
          case None => IO.println("  metrics server     = OFF")
        metrics <- makeMetrics(config.metrics, registry)
        engine = new ValidationEngine[TemperatureSensorEvent](config, metrics)
        now         <- IO.realTime.map(_.toMillis)
        heartbeat   <- Ref.of[IO, StreamHeartbeat](StreamHeartbeat(now))
        volumeState <- Ref.of[IO, VolumeSpikeDetector.State](VolumeSpikeDetector.State.empty)
        (transform, watchdog) = engine.build(heartbeat, volumeState)
        _ <-
          if config.simulationMode then runSimulation(transform, watchdog)
          else runServiceBus(config, transform, watchdog)
        snap <- metrics.snapshot
        _    <- IO.println(s"Final metrics snapshot: $snap")
        _ <-
          if config.metrics.prometheus.enabled && config.simulationMode then
            IO.println(
              s"Metrics server up for 5s — curl http://127.0.0.1:${config.metrics.prometheus.port}/metrics"
            ) *> IO.sleep(5.seconds)
          else IO.unit
      yield ()
    }

  private def makeMetrics(cfg: MetricsConfig, registry: PrometheusRegistry): IO[Metrics[IO]] =
    cfg.backend.toLowerCase match
      case "prometheus" => Metrics.prometheus(registry, echoToConsole = cfg.echoToConsole)
      case "silent"     => Metrics.silent
      case _            => Metrics.console

  private def runSimulation(
      transform: Stream[IO, String] => Stream[IO, RoutedEvent[TemperatureSensorEvent]],
      watchdog: Stream[IO, StatefulAlert]
  ): IO[Unit] =
    val now = System.currentTimeMillis()
    val samples = Stream(
      s"""{"deviceId":"sensor-01","temperature":22.5,"humidity":45.0,"timestamp":$now}""",
      s"""{"deviceId":"sensor-02","temperature":150.0,"humidity":40.0,"timestamp":${now + 1}}""",
      s"""{"deviceId":"sensor-03","temperature":18.0,"humidity":120.0,"timestamp":${now + 2}}""",
      s"""{"deviceId":"sensor-01","temperature":21.0,"humidity":44.0,"timestamp":${now + 3}}""",
      s"""{"deviceId":"sensor-01","temperature":5.0,"humidity":40.0,"timestamp":${now + 4}}""",
      s"""{"deviceId":"","temperature":20.0,"humidity":50.0,"timestamp":${now + 5}}""",
      """{not-json"""
    )

    val incoming =
      samples.metered[IO](300.millis) ++
        Stream.sleep[IO](6.seconds).drain ++
        Stream.emit(
          s"""{"deviceId":"sensor-99","temperature":23.0,"humidity":48.0,"timestamp":${System
              .currentTimeMillis()}}"""
        )

    val data = transform(incoming).evalMap { routed =>
      IO.println(s"  -> dest=${routed.targetTopic}  payload=${routed.payload.take(120)}")
    }

    IO.println("Running IoT simulation (no broker required)...") *>
      data.mergeHaltBoth(watchdog.drain).interruptAfter(14.seconds).compile.drain
  end runSimulation

  private def runServiceBus(
      config: EngineConfig,
      transform: Stream[IO, String] => Stream[IO, RoutedEvent[TemperatureSensorEvent]],
      watchdog: Stream[IO, StatefulAlert]
  ): IO[Unit] =
    val dest = config.messaging.destinations
    ServiceBuses.resource(config.messaging).use { bus =>
      val processed =
        bus.consume.evalMap { msg =>
          transform(Stream.emit(msg.payload)).compile.toList.flatMap {
            case routed :: _ => bus.publishAndAck(msg, routed)
            case Nil         => msg.ack
          }
        }
      IO.println(
        s"Consuming ${dest.incoming} via messaging.backend=${config.messaging.backend}"
      ) *> processed.mergeHaltBoth(watchdog.drain).compile.drain
    }
  end runServiceBus
end IoTStreamSpecsApp
