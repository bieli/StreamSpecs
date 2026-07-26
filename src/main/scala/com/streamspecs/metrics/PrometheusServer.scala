package com.streamspecs.metrics

import cats.effect.{IO, Resource}
import com.streamspecs.config.MetricsConfig
import io.prometheus.client.exporter.HTTPServer

object PrometheusServer:

  /** Start scrape endpoint; closed when the Resource is released. */
  def resource(cfg: MetricsConfig): Resource[IO, (PrometheusRegistry, Option[HTTPServer])] =
    Resource.make {
      IO.blocking {
        val registry = PrometheusRegistry.create(jvmMetrics = cfg.prometheus.jvmMetrics)
        val server =
          if cfg.prometheus.enabled then
            Some(
              PrometheusRegistry.startHttpServer(
                registry,
                cfg.prometheus.host,
                cfg.prometheus.port
              )
            )
          else None
        (registry, server)
      }
    } { case (_, server) =>
      IO.blocking(server.foreach(_.close())).void
    }
end PrometheusServer
