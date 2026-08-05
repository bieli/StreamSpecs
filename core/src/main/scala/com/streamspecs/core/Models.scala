package com.streamspecs.core

/** Severity of a failed / soft rule evaluation. */
enum Severity:
  case Error, Warning

final case class RuleIssue(rule: String, reason: String, severity: Severity)

/** Side-channel alerts from stateful / windowed engine checks. */
enum StatefulAlert:
  case TemporalAnomaly(reason: String, metricKey: String, idleForMs: Long)
  case RollingAverageAnomaly(
      metricName: String,
      currentAverage: Double,
      threshold: Double,
      metricKey: String,
      windowSize: Int
  )
  case TimeRollingAverageAnomaly(
      metricName: String,
      currentAverage: Double,
      threshold: Double,
      metricKey: String,
      windowMs: Long,
      sampleCount: Int
  )
  case DuplicateIdAnomaly(eventId: String, metricKey: String)
  case VolumeSpikeAnomaly(countInWindow: Int, maxAllowed: Int, metricKey: String, windowMs: Long)
  case MetricDeviationAnomaly(
      metricName: String,
      value: Double,
      baselineAverage: Double,
      deviationPercent: Double,
      maxAllowedPercent: Double,
      metricKey: String
  )
  case OutOfOrderAnomaly(eventTimestamp: Long, lastSeenTimestamp: Long, metricKey: String)
end StatefulAlert

/** Engine decision after combining user rules + routing config + stateful checks.
  *
  * @tparam T
  *   user domain event type
  */
enum EngineOutcome[T]:
  /** All rules passed (may still carry stateful alerts separately). */
  case Pass(event: T)

  /** Soft failures only — forward event, emit metrics / warnings. */
  case PassWithWarnings(event: T, issues: List[RuleIssue])

  /** Hard failure — route to DLQ (payload may be raw JSON if decode failed). */
  case Reject(rawPayload: String, issues: List[RuleIssue], event: Option[T])
