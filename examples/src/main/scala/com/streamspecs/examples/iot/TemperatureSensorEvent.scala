package com.streamspecs.examples.iot

import com.streamspecs.core.*
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

/** Example IoT domain event - no price / currency fields. */
final case class TemperatureSensorEvent(
    deviceId: String,
    temperature: Double,
    humidity: Double,
    timestamp: Long
)

object TemperatureSensorEvent:
  given Codec[TemperatureSensorEvent]      = deriveCodec
  given EventCodec[TemperatureSensorEvent] = EventCodec.fromCirce

  given DataQualityValidator[TemperatureSensorEvent] with
    def extractId(event: TemperatureSensorEvent): Option[String]      = Some(event.deviceId)
    def extractTimestamp(event: TemperatureSensorEvent): Option[Long] = Some(event.timestamp)

    def extractMetricValue(event: TemperatureSensorEvent, metricName: String): Option[Double] =
      metricName match
        case "temperature" => Some(event.temperature)
        case "humidity"    => Some(event.humidity)
        case _             => None

    def statelessRules(event: TemperatureSensorEvent): Map[String, RuleVerdict] =
      Map(
        "temperature-bound" -> (
          if event.temperature >= -50.0 && event.temperature <= 100.0 then RuleVerdict.Valid
          else RuleVerdict.Invalid(s"Temperature out of safe range: ${event.temperature}")
        ),
        "humidity-bound" -> (
          if event.humidity >= 0.0 && event.humidity <= 100.0 then RuleVerdict.Valid
          else RuleVerdict.Warning(s"Unusual humidity level: ${event.humidity}")
        ),
        "device-id-present" -> (
          if event.deviceId.nonEmpty then RuleVerdict.Valid
          else RuleVerdict.Invalid("deviceId is blank")
        )
      )
  end given
end TemperatureSensorEvent
