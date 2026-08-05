package com.streamspecs.core

/** Result of evaluating a single data-quality rule against an event. */
enum RuleVerdict:
  case Valid
  case Invalid(reason: String)
  case Warning(reason: String)

/** User-defined contract that binds an arbitrary domain type `T` to StreamSpecs.
  *
  * Implement this for your own events (IoT, finance, logistics, …). The engine never inspects your
  * fields directly - it only calls these methods.
  */
trait DataQualityValidator[T]:
  /** Stable event / device / entity identifier (used by duplicate-id checks). */
  def extractId(event: T): Option[String]

  /** Event-time epoch millis when available (freshness, out-of-order, time windows). */
  def extractTimestamp(event: T): Option[Long]

  /** Named numeric series for stateful / windowed checks. Example: `"temperature"`, `"humidity"`,
    * `"price"`.
    */
  def extractMetricValue(event: T, metricName: String): Option[Double]

  /** Stateless rule name -> verdict map evaluated for every event. */
  def statelessRules(event: T): Map[String, RuleVerdict]
end DataQualityValidator

object DataQualityValidator:
  def apply[T](using ev: DataQualityValidator[T]): DataQualityValidator[T] = ev
