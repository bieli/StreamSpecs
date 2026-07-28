package com.streamspecs.config

/** CLI overrides for the metrics HTTP scrape server and backend. */
final case class CliOptions(
    metricsServer: Option[Boolean] = None,
    metricsPort: Option[Int] = None,
    metricsHost: Option[String] = None,
    metricsBackend: Option[String] = None,
    help: Boolean = false,
    errors: List[String] = Nil
):
  def isValid: Boolean = errors.isEmpty

object CliOptions:

  private val Backends = Set("console", "silent", "prometheus")

  def parse(args: List[String]): CliOptions =
    @annotation.tailrec
    def loop(rest: List[String], acc: CliOptions): CliOptions =
      rest match
        case Nil              => acc
        case "--help" :: tail => loop(tail, acc.copy(help = true))
        case "-h" :: tail     => loop(tail, acc.copy(help = true))
        case "--metrics-server" :: tail =>
          loop(tail, acc.copy(metricsServer = Some(true)))
        case s"--metrics-server=$v" :: tail =>
          parseBool(v) match
            case Right(b)  => loop(tail, acc.copy(metricsServer = Some(b)))
            case Left(err) => loop(tail, acc.copy(errors = acc.errors :+ err))
        case "--no-metrics-server" :: tail =>
          loop(tail, acc.copy(metricsServer = Some(false)))
        case "--metrics-port" :: value :: tail =>
          value.toIntOption match
            case Some(p) if p > 0 && p < 65536 =>
              loop(tail, acc.copy(metricsPort = Some(p)))
            case _ =>
              loop(tail, acc.copy(errors = acc.errors :+ s"Invalid --metrics-port value: '$value'"))
        case s"--metrics-port=$value" :: tail =>
          loop("--metrics-port" :: value :: tail, acc)
        case "--metrics-host" :: value :: tail =>
          loop(tail, acc.copy(metricsHost = Some(value)))
        case s"--metrics-host=$value" :: tail =>
          loop(tail, acc.copy(metricsHost = Some(value)))
        case "--metrics-backend" :: value :: tail =>
          val normalized = value.toLowerCase
          if Backends.contains(normalized) then
            loop(tail, acc.copy(metricsBackend = Some(normalized)))
          else
            loop(
              tail,
              acc.copy(errors =
                acc.errors :+ s"Invalid --metrics-backend '$value' (use: ${Backends.mkString("|")})"
              )
            )
        case s"--metrics-backend=$value" :: tail =>
          loop("--metrics-backend" :: value :: tail, acc)
        case unknown :: tail =>
          loop(tail, acc.copy(errors = acc.errors :+ s"Unknown argument: $unknown"))
    loop(args, CliOptions())
  end parse

  def helpText: String =
    """StreamSpecs — universal streaming data-quality engine
      |
      |Usage:
      |  <your-app> [options]
      |
      |Metrics server (Prometheus scrape endpoint):
      |  --metrics-server[=true|false]  Enable/disable HTTP /metrics server
      |  --no-metrics-server            Disable HTTP /metrics server
      |  --metrics-port <port>          Scrape port (default: 9464)
      |  --metrics-host <host>          Bind address (default: 0.0.0.0)
      |  --metrics-backend <name>       console | silent | prometheus
      |
      |Environment (overridden by CLI):
      |  STREAMSPECS_METRICS_SERVER, STREAMSPECS_METRICS_PORT,
      |  STREAMSPECS_METRICS_HOST, STREAMSPECS_METRICS_BACKEND
      |
      |Precedence: CLI > environment > application.conf
      |""".stripMargin

  private def parseBool(raw: String): Either[String, Boolean] =
    raw.trim.toLowerCase match
      case "true" | "1" | "yes" | "on"  => Right(true)
      case "false" | "0" | "no" | "off" => Right(false)
      case other                        => Left(s"Invalid boolean '$other' (expected true|false)")

  def applyTo(config: EngineConfig, cli: CliOptions): EngineConfig =
    val m = config.metrics
    val p = m.prometheus
    config.copy(
      metrics = m.copy(
        backend = cli.metricsBackend.getOrElse(m.backend),
        prometheus = p.copy(
          enabled = cli.metricsServer.getOrElse(p.enabled),
          port = cli.metricsPort.getOrElse(p.port),
          host = cli.metricsHost.getOrElse(p.host)
        )
      )
    )
  end applyTo
end CliOptions
