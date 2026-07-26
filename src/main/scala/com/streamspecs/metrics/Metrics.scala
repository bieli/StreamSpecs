package com.streamspecs.metrics

import cats.effect.IO
import cats.effect.kernel.Ref

/** Metrics facade used by the validation pipeline. */
trait Metrics[F[_]]:
  def increment(key: String): F[Unit]
  def snapshot: F[Map[String, Long]]

object Metrics:

  def console: IO[Metrics[IO]] =
    Ref.of[IO, Map[String, Long]](Map.empty).map { counters =>
      new Metrics[IO]:
        def increment(key: String): IO[Unit] =
          counters.update(m => m.updatedWith(key)(c => Some(c.getOrElse(0L) + 1L))) *>
            IO.println(s"[METRIC] +1  $key")

        def snapshot: IO[Map[String, Long]] = counters.get
    }

  def silent: IO[Metrics[IO]] =
    Ref.of[IO, Map[String, Long]](Map.empty).map { counters =>
      new Metrics[IO]:
        def increment(key: String): IO[Unit] =
          counters.update(m => m.updatedWith(key)(c => Some(c.getOrElse(0L) + 1L)))

        def snapshot: IO[Map[String, Long]] = counters.get
    }

  /** Prometheus-backed metrics (counters + optional console echo). */
  def prometheus(
      registry: PrometheusRegistry,
      echoToConsole: Boolean = false
  ): IO[Metrics[IO]] =
    Ref.of[IO, Map[String, Long]](Map.empty).map { local =>
      new Metrics[IO]:
        def increment(key: String): IO[Unit] =
          local.update(m => m.updatedWith(key)(c => Some(c.getOrElse(0L) + 1L))) *>
            IO.blocking(registry.inc(key)) *>
            (if echoToConsole then IO.println(s"[METRIC] +1  $key") else IO.unit)

        def snapshot: IO[Map[String, Long]] = local.get
    }
end Metrics
