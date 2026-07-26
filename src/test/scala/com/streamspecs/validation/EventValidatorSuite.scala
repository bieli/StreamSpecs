package com.streamspecs

import com.streamspecs.config.RuleAction
import com.streamspecs.domain.ValidationOutcome
import com.streamspecs.validation.EventValidator
import munit.FunSuite

class EventValidatorSuite extends FunSuite:

  private val rules = Map(
    "missing-id"           -> RuleAction("alerts.errors.missing_id", sendToDlq = true),
    "negative-price"       -> RuleAction("alerts.errors.negative_price", sendToDlq = true),
    "invalid-email-format" -> RuleAction("alerts.warnings.invalid_email", sendToDlq = false),
    "json-parse-failure"   -> RuleAction("alerts.errors.json_parse", sendToDlq = true)
  )

  private val validator = new EventValidator(rules)

  test("valid event passes") {
    val raw = """{"id":"ORD-001","price":10.5,"email":"a@b.com"}"""
    validator.validate(raw) match
      case ValidationOutcome.Valid(e) =>
        assertEquals(e.id, Some("ORD-001"))
        assertEquals(e.price, 10.5)
      case other => fail(s"expected Valid, got $other")
  }

  test("missing id goes to DLQ when send-to-dlq=true") {
    val raw = """{"id":"","price":10.5,"email":"a@b.com"}"""
    validator.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "missing-id", metricKey, _) =>
        assertEquals(metricKey, "alerts.errors.missing_id")
      case other => fail(s"expected DeadLetter(missing-id), got $other")
  }

  test("negative price goes to DLQ") {
    val raw = """{"id":"ORD-2","price":-1.0,"email":"a@b.com"}"""
    validator.validate(raw) match
      case ValidationOutcome.DeadLetter(_, "negative-price", _, _) => ()
      case other => fail(s"expected DeadLetter(negative-price), got $other")
  }

  test("invalid email passes downstream when send-to-dlq=false") {
    val raw = """{"id":"ORD-3","price":10.0,"email":"bad-email"}"""
    validator.validate(raw) match
      case ValidationOutcome.InvalidButPass(_, "invalid-email-format", metricKey, _) =>
        assertEquals(metricKey, "alerts.warnings.invalid_email")
      case other => fail(s"expected InvalidButPass, got $other")
  }

  test("malformed JSON goes to DLQ") {
    validator.validate("{nope") match
      case ValidationOutcome.DeadLetter(_, "json-parse-failure", _, _) => ()
      case other => fail(s"expected DeadLetter(json-parse-failure), got $other")
  }
end EventValidatorSuite
