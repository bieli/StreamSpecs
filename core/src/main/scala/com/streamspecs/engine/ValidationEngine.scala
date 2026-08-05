package com.streamspecs.engine

import cats.effect.IO
import cats.effect.kernel.Ref
import com.streamspecs.config.EngineConfig
import com.streamspecs.core.*
import com.streamspecs.metrics.Metrics
import com.streamspecs.validation.*
import fs2.Stream
import io.circe.syntax.*

import java.time.Instant

final case class RoutedEvent[T](
    outcome: EngineOutcome[T],
    targetTopic: String,
    payload: String,
    metricKeys: List[String]
)

/** Domain-agnostic streaming validation engine. */
final class ValidationEngine[T](config: EngineConfig, metrics: Metrics[IO])(using
    DataQualityValidator[T],
    EventCodec[T]
):

  private val evaluator = new StatelessEvaluator[T](config)

  def build(
      heartbeat: Ref[IO, StreamHeartbeat],
      volumeState: Ref[IO, VolumeSpikeDetector.State]
  ): (Stream[IO, String] => Stream[IO, RoutedEvent[T]], Stream[IO, StatefulAlert]) =
    val validTopic = config.messaging.destinations.valid
    val dlqTopic   = config.messaging.destinations.dlq

    val stateful: StatefulPipe.PipeLegacy[T] =
      StatefulPipe.sequence(
        List(
          config.statefulRules.duplicateIdCheck.map(DuplicateIdValidator.stage[T]),
          config.statefulRules.outOfOrderCheck.map(OutOfOrderValidator.stage[T]),
          config.statefulRules.rollingAverageCheck.map(RollingAverageValidator.stage[T]),
          config.statefulRules.timeRollingAverageCheck.map(TimeRollingAverageValidator.stage[T]),
          config.statefulRules.metricDeviationCheck.map(MetricDeviationValidator.stage[T])
        ).flatten
      )

    val dataTransform: Stream[IO, String] => Stream[IO, RoutedEvent[T]] =
      incoming =>
        val validated =
          incoming.evalMap { raw =>
            val markVolume =
              config.statefulRules.volumeSpikeCheck match
                case Some(rule) =>
                  VolumeSpikeDetector.markEvent(volumeState, rule.windowDuration.toMillis)
                case None => IO.unit
            DeadMansSwitch.markAlive(heartbeat) *> markVolume *> IO.delay(evaluator.evaluate(raw))
          }

        stateful(validated).evalMap { case (outcome, alerts) =>
          for
            routed <- route(outcome, validTopic, dlqTopic)
            _      <- routed.metricKeys.traverse_(metrics.increment)
            _      <- alerts.traverse_(a => handleStatefulAlert(a))
          yield routed
        }

    val heartbeatWatch =
      config.statefulRules.heartbeatCheck match
        case Some(rule) =>
          DeadMansSwitch.watchdog(heartbeat, rule).evalTap(handleStatefulAlert)
        case None => Stream.empty

    val volumeWatch =
      config.statefulRules.volumeSpikeCheck match
        case Some(rule) =>
          VolumeSpikeDetector.watchdog(volumeState, rule).evalTap(handleStatefulAlert)
        case None => Stream.empty

    (dataTransform, heartbeatWatch.merge(volumeWatch))
  end build

  private def route(
      outcome: EngineOutcome[T],
      validTopic: String,
      dlqTopic: String
  ): IO[RoutedEvent[T]] =
    outcome match
      case EngineOutcome.Pass(event) =>
        IO.pure(
          RoutedEvent(
            outcome,
            validTopic,
            summon[EventCodec[T]].encode(event),
            evaluator.metricKeysFor(outcome)
          )
        )
      case EngineOutcome.PassWithWarnings(event, issues) =>
        IO.println(s"[WARN] ${issues.map(i => s"${i.rule}: ${i.reason}").mkString("; ")}") *>
          IO.pure(
            RoutedEvent(
              outcome,
              validTopic,
              summon[EventCodec[T]].encode(event),
              evaluator.metricKeysFor(outcome)
            )
          )
      case EngineOutcome.Reject(raw, issues, _) =>
        val envelope =
          Map(
            "raw"    -> raw,
            "issues" -> issues.map(i => s"${i.rule}:${i.severity}:${i.reason}").mkString("|"),
            "ts"     -> Instant.now().toEpochMilli.toString
          ).asJson.noSpaces
        IO.println(s"[DLQ] ${issues.map(_.reason).mkString("; ")}") *>
          IO.pure(RoutedEvent(outcome, dlqTopic, envelope, evaluator.metricKeysFor(outcome)))

  private def handleStatefulAlert(alert: StatefulAlert): IO[Unit] =
    alert match
      case StatefulAlert.TemporalAnomaly(reason, metricKey, idleForMs) =>
        IO.println(s"[ALERT] Dead Man's Switch: $reason (idle=${idleForMs}ms)") *>
          metrics.increment(metricKey) *> metrics.increment("alerts.stateful.temporal")
      case StatefulAlert.RollingAverageAnomaly(metricName, avg, threshold, metricKey, windowSize) =>
        IO.println(
          f"[ALERT] Rolling avg($metricName)=$avg%.2f < $threshold%.2f (window=$windowSize)"
        ) *> metrics.increment(metricKey) *> metrics.increment("alerts.stateful.rolling_average")
      case StatefulAlert.TimeRollingAverageAnomaly(
            metricName,
            avg,
            threshold,
            metricKey,
            windowMs,
            n
          ) =>
        IO.println(
          f"[ALERT] Time-rolling avg($metricName)=$avg%.2f < $threshold%.2f (window=${windowMs}ms, n=$n)"
        ) *> metrics.increment(metricKey) *> metrics.increment(
          "alerts.stateful.time_rolling_average"
        )
      case StatefulAlert.DuplicateIdAnomaly(eventId, metricKey) =>
        IO.println(s"[ALERT] Duplicate id: $eventId") *>
          metrics.increment(metricKey) *> metrics.increment("alerts.stateful.duplicate_id")
      case StatefulAlert.VolumeSpikeAnomaly(count, max, metricKey, windowMs) =>
        IO.println(s"[ALERT] Volume spike: $count / ${windowMs}ms (max=$max)") *>
          metrics.increment(metricKey) *> metrics.increment("alerts.stateful.volume_spike")
      case StatefulAlert.MetricDeviationAnomaly(
            metricName,
            value,
            baseline,
            pct,
            maxPct,
            metricKey
          ) =>
        IO.println(
          f"[ALERT] Deviation($metricName): value=$value%.2f baseline=$baseline%.2f ($pct%.1f%% > $maxPct%.1f%%)"
        ) *> metrics.increment(metricKey) *> metrics.increment("alerts.stateful.metric_deviation")
      case StatefulAlert.OutOfOrderAnomaly(ts, last, metricKey) =>
        IO.println(s"[ALERT] Out-of-order timestamp: $ts < $last") *>
          metrics.increment(metricKey) *> metrics.increment("alerts.stateful.out_of_order")

  extension [A](xs: List[A])
    private def traverse_(f: A => IO[Unit]): IO[Unit] =
      xs.foldLeft(IO.unit)((acc, a) => acc *> f(a))
end ValidationEngine
