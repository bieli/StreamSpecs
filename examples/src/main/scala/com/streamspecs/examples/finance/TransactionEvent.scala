package com.streamspecs.examples.finance

import com.streamspecs.core.*
import com.streamspecs.util.Iso4217
import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

/** Optional finance domain example - shows ISO 4217 usage outside the library core. */
final case class TransactionEvent(
    id: Option[String],
    price: Double,
    email: String,
    currency: Option[String],
    eventTimestamp: Option[Long]
)

object TransactionEvent:
  given Codec[TransactionEvent]      = deriveCodec
  given EventCodec[TransactionEvent] = EventCodec.fromCirce

  private val Email =
    "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$".r

  given DataQualityValidator[TransactionEvent] with
    def extractId(event: TransactionEvent): Option[String] =
      event.id.filter(_.nonEmpty)

    def extractTimestamp(event: TransactionEvent): Option[Long] =
      event.eventTimestamp

    def extractMetricValue(event: TransactionEvent, metricName: String): Option[Double] =
      metricName match
        case "price" => Some(event.price)
        case _       => None

    def statelessRules(event: TransactionEvent): Map[String, RuleVerdict] =
      Map(
        "missing-id" -> (
          if event.id.forall(_.isBlank) then RuleVerdict.Invalid("Field 'id' is missing or blank")
          else RuleVerdict.Valid
        ),
        "negative-price" -> (
          if event.price <= 0 then RuleVerdict.Invalid(s"Invalid price: ${event.price}")
          else RuleVerdict.Valid
        ),
        "invalid-email-format" -> (
          if Email.matches(event.email) then RuleVerdict.Valid
          else RuleVerdict.Warning(s"Invalid email format: ${event.email}")
        ),
        "invalid-iso4217-currency" -> (
          event.currency match
            case None | Some("") => RuleVerdict.Valid // optional
            case Some(code) if !Iso4217.isAlpha3Format(code) =>
              RuleVerdict.Invalid(s"Currency '$code' is not a 3-letter ISO 4217 code")
            case Some(code) if !Iso4217.isValid(code) =>
              RuleVerdict.Invalid(s"Currency '${code.toUpperCase}' is not a valid ISO 4217 code")
            case Some(_) => RuleVerdict.Valid
        )
      )
  end given
end TransactionEvent
