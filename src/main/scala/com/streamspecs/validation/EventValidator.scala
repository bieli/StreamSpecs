package com.streamspecs.validation

import com.streamspecs.config.{AllowedCurrencyRule, FreshnessRule, Iso4217CurrencyRule, RuleAction}
import com.streamspecs.domain.{TransactionEvent, ValidationOutcome}
import io.circe.parser.decode

import java.util.regex.Pattern
import scala.concurrent.duration.*

/** Stateless field validators driven by HOCON rule actions (metric-key + send-to-dlq). */
final class EventValidator(
    rules: Map[String, RuleAction],
    freshness: Option[FreshnessRule] = None,
    iso4217Currency: Iso4217CurrencyRule = Iso4217CurrencyRule.default,
    allowedCurrency: Option[AllowedCurrencyRule] = None,
    nowMs: () => Long = () => System.currentTimeMillis()
):

  private val emailPattern: Pattern =
    Pattern.compile("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")

  private def strategy(errorCode: String): RuleAction =
    rules.getOrElse(errorCode, RuleAction(s"unknown.error.$errorCode", sendToDlq = true))

  private def decide(
      errorCode: String,
      raw: String,
      event: TransactionEvent,
      reason: String,
      overrideAction: Option[RuleAction] = None
  ): ValidationOutcome =
    val action = overrideAction.getOrElse(strategy(errorCode))
    if action.sendToDlq then ValidationOutcome.DeadLetter(raw, errorCode, action.metricKey, reason)
    else ValidationOutcome.InvalidButPass(event, errorCode, action.metricKey, reason)
  end decide

  def validate(rawJson: String): ValidationOutcome =
    decode[TransactionEvent](rawJson) match
      case Left(err) =>
        val action = strategy("json-parse-failure")
        ValidationOutcome.DeadLetter(
          rawJson,
          "json-parse-failure",
          action.metricKey,
          s"JSON parse error: ${err.getMessage}"
        )

      case Right(event) =>
        validateEvent(rawJson, event)

  private def validateEvent(raw: String, event: TransactionEvent): ValidationOutcome =
    if event.id.forall(_.isBlank) then
      decide("missing-id", raw, event, "Field 'id' is missing or blank")
    else if event.price <= 0 then
      decide("negative-price", raw, event, s"Invalid price: ${event.price}")
    else if !emailPattern.matcher(event.email).matches() then
      decide("invalid-email-format", raw, event, s"Invalid email format: ${event.email}")
    else
      checkIso4217(raw, event)
        .orElse(checkAllowedCurrency(raw, event))
        .orElse(checkFreshness(raw, event))
        .getOrElse(ValidationOutcome.Valid(event))

  /** Default banking/insurance check: currency must be ISO 4217 alpha-3 when present. */
  private def checkIso4217(raw: String, event: TransactionEvent): Option[ValidationOutcome] =
    if !iso4217Currency.enabled then None
    else
      val action = RuleAction(iso4217Currency.metricKey, iso4217Currency.sendToDlq)
      event.currency.map(_.trim) match
        case None | Some("") if iso4217Currency.requireField =>
          Some(
            decide(
              "missing-currency",
              raw,
              event,
              "Field 'currency' is required (ISO 4217)",
              Some(action)
            )
          )
        case None | Some("") =>
          None
        case Some(code) if !Iso4217.isAlpha3Format(code) =>
          Some(
            decide(
              "invalid-iso4217-format",
              raw,
              event,
              s"Currency '$code' is not a 3-letter ISO 4217 alphabetic code",
              Some(action)
            )
          )
        case Some(code) if !Iso4217.isValid(code) =>
          Some(
            decide(
              "invalid-iso4217-currency",
              raw,
              event,
              s"Currency '${code.toUpperCase}' is not a valid ISO 4217 currency code",
              Some(action)
            )
          )
        case Some(_) =>
          None
      end match
  /** Optional business allow-list applied after ISO 4217. */
  private def checkAllowedCurrency(
      raw: String,
      event: TransactionEvent
  ): Option[ValidationOutcome] =
    allowedCurrency.flatMap { rule =>
      event.currency.map(_.trim.toUpperCase) match
        case None | Some("") =>
          Some(
            decide(
              "missing-currency",
              raw,
              event,
              "Field 'currency' is missing",
              Some(RuleAction(rule.metricKey, rule.sendToDlq))
            )
          )
        case Some(c) if !rule.allowed.map(_.toUpperCase).contains(c) =>
          Some(
            decide(
              "invalid-currency",
              raw,
              event,
              s"Currency '$c' not in allowed set ${rule.allowed}",
              Some(RuleAction(rule.metricKey, rule.sendToDlq))
            )
          )
        case _ => None
    }

  private def checkFreshness(raw: String, event: TransactionEvent): Option[ValidationOutcome] =
    freshness.flatMap { rule =>
      event.eventTimestamp match
        case None => None
        case Some(ts) =>
          val lag = (nowMs() - ts).millis
          if lag > rule.maxLag then
            Some(
              decide(
                "stale-event",
                raw,
                event,
                s"Event lag ${lag.toMillis}ms exceeds max ${rule.maxLag.toMillis}ms",
                Some(RuleAction(rule.metricKey, rule.sendToDlq))
              )
            )
          else None
          end if
    }
end EventValidator
