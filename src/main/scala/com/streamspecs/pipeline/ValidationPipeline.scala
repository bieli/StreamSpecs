package com.streamspecs.pipeline

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.{Iso4217CurrencyRule, StreamValidatorConfig}
import com.streamspecs.domain.{StatefulAlert, ValidationOutcome}
import com.streamspecs.metrics.Metrics
import com.streamspecs.validation.*
import fs2.Stream
import io.circe.syntax.*

import java.time.Instant

/** Core validation pipeline: stateless + stateful DQ checks + watchdog streams. */
object ValidationPipeline:

  final case class RoutedEvent(
      outcome: ValidationOutcome,
      targetTopic: String,
      payload: String,
      metricKeys: List[String]
  )

  def build(
      config: StreamValidatorConfig,
      metrics: Metrics[IO],
      heartbeat: Ref[IO, StreamHeartbeat],
      volumeState: Ref[IO, VolumeSpikeDetector.State]
  ): (
      Stream[IO, String] => Stream[IO, RoutedEvent],
      Stream[IO, StatefulAlert]
  ) =
    val validator = new EventValidator(
      rules = config.rules,
      freshness = config.statelessExtras.freshnessCheck,
      iso4217Currency =
        config.statelessExtras.iso4217CurrencyCheck.getOrElse(Iso4217CurrencyRule.default),
      allowedCurrency = config.statelessExtras.allowedCurrencyCheck
    )
    val validTopic = config.kafka.topics.valid
    val dlqTopic   = config.kafka.topics.dlq

    val statefulPipe: StatefulPipe.PipeLegacy =
      StatefulPipe.sequence(
        List(
          config.statefulRules.duplicateIdCheck.map(DuplicateIdValidator.stage),
          config.statefulRules.outOfOrderCheck.map(OutOfOrderValidator.stage),
          config.statefulRules.rollingPriceCheck.map(RollingAverageValidator.stage),
          config.statefulRules.timeRollingPriceCheck.map(TimeRollingAverageValidator.stage),
          config.statefulRules.priceDeviationCheck.map(PriceDeviationValidator.stage)
        ).flatten
      )

    val dataTransform: Stream[IO, String] => Stream[IO, RoutedEvent] =
      incoming =>
        val validated: Stream[IO, ValidationOutcome] =
          incoming.evalMap { raw =>
            val markVolume =
              config.statefulRules.volumeSpikeCheck match
                case Some(rule) =>
                  VolumeSpikeDetector.markEvent(volumeState, rule.windowDuration.toMillis)
                case None => IO.unit
            DeadMansSwitch.markAlive(heartbeat) *>
              markVolume *>
              IO.delay(validator.validate(raw))
          }

        statefulPipe(validated).evalMap { case (outcome, alerts) =>
          for
            routed <- route(outcome, validTopic, dlqTopic)
            _      <- routed.metricKeys.traverse_(metrics.increment)
            _      <- alerts.traverse_(a => handleStatefulAlert(a, metrics))
          yield routed
        }

    val heartbeatWatch: Stream[IO, StatefulAlert] =
      config.statefulRules.heartbeatCheck match
        case Some(rule) =>
          DeadMansSwitch.watchdog(heartbeat, rule).evalTap(a => handleStatefulAlert(a, metrics))
        case None => Stream.empty

    val volumeWatch: Stream[IO, StatefulAlert] =
      config.statefulRules.volumeSpikeCheck match
        case Some(rule) =>
          VolumeSpikeDetector
            .watchdog(volumeState, rule)
            .evalTap(a => handleStatefulAlert(a, metrics))
        case None => Stream.empty

    (dataTransform, heartbeatWatch.merge(volumeWatch))
  end build

  private def route(
      outcome: ValidationOutcome,
      validTopic: String,
      dlqTopic: String
  ): IO[RoutedEvent] =
    outcome match
      case ValidationOutcome.Valid(event) =>
        IO.pure(
          RoutedEvent(
            outcome,
            validTopic,
            event.asJson.noSpaces,
            metricKeys = List("events.valid")
          )
        )

      case ValidationOutcome.InvalidButPass(event, _, metricKey, reason) =>
        IO.println(s"[WARN] Passing with anomaly ($reason)") *>
          IO.pure(
            RoutedEvent(
              outcome,
              validTopic,
              event.asJson.noSpaces,
              metricKeys = List(metricKey, "events.pass_with_warning")
            )
          )

      case ValidationOutcome.DeadLetter(raw, errorCode, metricKey, reason) =>
        val envelope =
          Map(
            "raw"       -> raw,
            "reason"    -> reason,
            "errorCode" -> errorCode,
            "metricKey" -> metricKey,
            "ts"        -> Instant.now().toEpochMilli.toString
          ).asJson.noSpaces
        IO.println(s"[DLQ] $errorCode: $reason") *>
          IO.pure(
            RoutedEvent(
              outcome,
              dlqTopic,
              envelope,
              metricKeys = List(metricKey, "events.dlq")
            )
          )

  private def handleStatefulAlert(alert: StatefulAlert, metrics: Metrics[IO]): IO[Unit] =
    alert match
      case StatefulAlert.TemporalAnomaly(reason, metricKey, idleForMs) =>
        IO.println(s"[ALERT] Dead Man's Switch: $reason (idle=${idleForMs}ms)") *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.temporal")

      case StatefulAlert.RollingAverageAnomaly(avg, threshold, metricKey, windowSize) =>
        IO.println(
          f"[ALERT] Rolling average $avg%.2f < $threshold%.2f (window=$windowSize)"
        ) *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.rolling_average")

      case StatefulAlert.DuplicateIdAnomaly(eventId, metricKey) =>
        IO.println(s"[ALERT] Duplicate id detected: $eventId") *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.duplicate_id")

      case StatefulAlert.VolumeSpikeAnomaly(count, max, metricKey, windowMs) =>
        IO.println(s"[ALERT] Volume spike: $count events / ${windowMs}ms (max=$max)") *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.volume_spike")

      case StatefulAlert.PriceDeviationAnomaly(price, baseline, pct, maxPct, metricKey) =>
        IO.println(
          f"[ALERT] Price deviation: price=$price%.2f baseline=$baseline%.2f deviation=$pct%.1f%% (max=$maxPct%.1f%%)"
        ) *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.price_deviation")

      case StatefulAlert.OutOfOrderAnomaly(ts, last, metricKey) =>
        IO.println(s"[ALERT] Out-of-order timestamp: $ts < last $last") *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.out_of_order")

      case StatefulAlert.TimeRollingAverageAnomaly(avg, threshold, metricKey, windowMs, samples) =>
        IO.println(
          f"[ALERT] Time-rolling average $avg%.2f < $threshold%.2f (window=${windowMs}ms, n=$samples)"
        ) *>
          metrics.increment(metricKey) *>
          metrics.increment("alerts.stateful.time_rolling_average")

  extension [A](xs: List[A])
    private def traverse_(f: A => IO[Unit]): IO[Unit] =
      xs.foldLeft(IO.unit)((acc, a) => acc *> f(a))
end ValidationPipeline
