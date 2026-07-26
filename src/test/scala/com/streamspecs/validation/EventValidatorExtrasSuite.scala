package com.streamspecs

import com.streamspecs.config.{AllowedCurrencyRule, FreshnessRule, Iso4217CurrencyRule, RuleAction}
import com.streamspecs.domain.ValidationOutcome
import com.streamspecs.validation.EventValidator
import munit.FunSuite

import scala.concurrent.duration.*

class EventValidatorExtrasSuite extends FunSuite:

  private val baseRules = Map(
    "missing-id"           -> RuleAction("alerts.errors.missing_id", sendToDlq = true),
    "negative-price"       -> RuleAction("alerts.errors.negative_price", sendToDlq = true),
    "invalid-email-format" -> RuleAction("alerts.warnings.invalid_email", sendToDlq = false),
    "json-parse-failure"   -> RuleAction("alerts.errors.json_parse", sendToDlq = true)
  )

  test("ISO 4217 rejects non-currency tickers like BTC") {
    val v   = new EventValidator(baseRules) // default iso4217 enabled
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"BTC"}"""
    v.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "invalid-iso4217-currency", _, reason) =>
        assert(reason.contains("ISO 4217"))
      case other => fail(s"expected invalid-iso4217-currency DLQ, got $other")
  }

  test("ISO 4217 rejects non alpha-3 format") {
    val v   = new EventValidator(baseRules)
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"EU"}"""
    v.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "invalid-iso4217-format", _, _) => ()
      case other => fail(s"expected invalid-iso4217-format, got $other")
  }

  test("ISO 4217 accepts PLN EUR USD") {
    val v = new EventValidator(baseRules)
    List("PLN", "EUR", "USD", "pln").foreach { code =>
      val raw = s"""{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"$code"}"""
      v.validate(raw) match
        case ValidationOutcome.Valid(_) => ()
        case other                      => fail(s"expected Valid for $code, got $other")
    }
  }

  test("ISO 4217 can require currency field") {
    val v = new EventValidator(
      baseRules,
      iso4217Currency = Iso4217CurrencyRule.default.copy(requireField = true)
    )
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com"}"""
    v.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "missing-currency", _, _) => ()
      case other => fail(s"expected missing-currency, got $other")
  }

  test("allowed-currency further restricts after ISO 4217") {
    val v = new EventValidator(
      baseRules,
      allowedCurrency = Some(
        AllowedCurrencyRule("alerts.errors.invalid_currency", List("PLN", "EUR"), sendToDlq = true)
      )
    )
    // GBP is ISO 4217 but not in product allow-list
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"GBP"}"""
    v.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "invalid-currency", _, _) => ()
      case other => fail(s"expected invalid-currency DLQ, got $other")
  }

  test("flags stale events by freshness lag") {
    val now = 1_700_000_000_000L
    val v = new EventValidator(
      baseRules,
      freshness =
        Some(FreshnessRule("alerts.warnings.stale_event", maxLag = 1.minute, sendToDlq = false)),
      nowMs = () => now
    )
    val staleTs = now - 5.minutes.toMillis
    val raw =
      s"""{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"PLN","eventTimestamp":$staleTs}"""
    v.validate(raw) match
      case ValidationOutcome.InvalidButPass(_, "stale-event", _, _) => ()
      case other => fail(s"expected stale-event pass-with-warn, got $other")
  }

  test("fresh events within max-lag pass freshness check") {
    val now = 1_700_000_000_000L
    val v = new EventValidator(
      baseRules,
      freshness =
        Some(FreshnessRule("alerts.warnings.stale_event", maxLag = 5.minutes, sendToDlq = true)),
      nowMs = () => now
    )
    val raw =
      s"""{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"PLN","eventTimestamp":${now - 1000}}"""
    v.validate(raw) match
      case ValidationOutcome.Valid(_) => ()
      case other                      => fail(s"expected Valid, got $other")
  }

  test("ISO 4217 disabled allows non-ISO tickers through") {
    val v = new EventValidator(
      baseRules,
      iso4217Currency = Iso4217CurrencyRule.default.copy(enabled = false)
    )
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"BTC"}"""
    v.validate(raw) match
      case ValidationOutcome.Valid(_) => ()
      case other                      => fail(s"expected Valid when ISO disabled, got $other")
  }

  test("missing currency is OK when requireField=false") {
    val v   = new EventValidator(baseRules)
    val raw = """{"id":"ORD-1","price":10.0,"email":"a@b.com"}"""
    v.validate(raw) match
      case ValidationOutcome.Valid(_) => ()
      case other                      => fail(s"expected Valid, got $other")
  }

  test("stale event can be routed to DLQ when sendToDlq=true") {
    val now = 1_700_000_000_000L
    val v = new EventValidator(
      baseRules,
      freshness =
        Some(FreshnessRule("alerts.warnings.stale_event", maxLag = 1.minute, sendToDlq = true)),
      nowMs = () => now
    )
    val raw =
      s"""{"id":"ORD-1","price":10.0,"email":"a@b.com","currency":"PLN","eventTimestamp":${now - 10.minutes.toMillis}}"""
    v.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "stale-event", _, _) => ()
      case other => fail(s"expected stale-event DLQ, got $other")
  }
end EventValidatorExtrasSuite
