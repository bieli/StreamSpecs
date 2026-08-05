package com.streamspecs.validation

import com.streamspecs.config.{EngineConfig, RuleAction}
import com.streamspecs.core.*

/** Applies user [[DataQualityValidator.statelessRules]] and maps them to engine outcomes using
  * HOCON routing (`send-to-dlq` / `metric-key` per rule name).
  */
final class StatelessEvaluator[T](config: EngineConfig)(using
    validator: DataQualityValidator[T],
    codec: EventCodec[T]
):

  private def actionFor(rule: String): RuleAction =
    config.rules.getOrElse(
      rule,
      RuleAction(
        metricKey = s"alerts.errors.${rule.replace('-', '_')}",
        sendToDlq = config.defaultSendToDlq
      )
    )

  def evaluate(raw: String): EngineOutcome[T] =
    summon[EventCodec[T]].decode(raw) match
      case Left(err) =>
        EngineOutcome.Reject(
          rawPayload = raw,
          issues = List(RuleIssue("decode-failure", err, Severity.Error)),
          event = None
        )
      case Right(event) =>
        evaluateEvent(raw, event)

  def evaluateEvent(raw: String, event: T): EngineOutcome[T] =
    val results = validator.statelessRules(event)
    val errors = results.collect { case (name, RuleVerdict.Invalid(reason)) =>
      RuleIssue(name, reason, Severity.Error)
    }.toList
    val warnings = results.collect { case (name, RuleVerdict.Warning(reason)) =>
      RuleIssue(name, reason, Severity.Warning)
    }.toList

    val hardReject = errors.exists { issue =>
      actionFor(issue.rule).sendToDlq
    }

    if hardReject then EngineOutcome.Reject(raw, errors ++ warnings, Some(event))
    else if errors.nonEmpty || warnings.nonEmpty then
      // Soft errors (sendToDlq=false) are treated like warnings for routing
      val soft = errors.map(_.copy(severity = Severity.Warning)) ++ warnings
      if soft.nonEmpty then EngineOutcome.PassWithWarnings(event, soft)
      else EngineOutcome.Pass(event)
    else EngineOutcome.Pass(event)
  end evaluateEvent

  def metricKeysFor(outcome: EngineOutcome[T]): List[String] =
    outcome match
      case EngineOutcome.Pass(_) => List("events.valid")
      case EngineOutcome.PassWithWarnings(_, issues) =>
        issues.map(i => actionFor(i.rule).metricKey) :+ "events.pass_with_warning"
      case EngineOutcome.Reject(_, issues, _) =>
        val keys =
          if issues.isEmpty then List(actionFor("decode-failure").metricKey)
          else issues.map(i => actionFor(i.rule).metricKey)
        keys :+ "events.dlq"
end StatelessEvaluator
