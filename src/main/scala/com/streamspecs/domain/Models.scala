package com.streamspecs.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec

/** Incoming transaction / event payload parsed from Kafka JSON. */
final case class TransactionEvent(
    id: Option[String],
    price: Double,
    email: String,
    currency: Option[String] = None,
    eventTimestamp: Option[Long] = None
)

object TransactionEvent:
  given Codec[TransactionEvent] = deriveCodec

/** Envelope written to the Dead Letter Queue topic. */
final case class DlqEnvelope(
    raw: String,
    reason: String,
    metricKey: String,
    errorCode: String,
    timestampMs: Long
)

object DlqEnvelope:
  given Codec[DlqEnvelope] = deriveCodec

/** Outcome of validating a single event (stateless + routing strategy). */
enum ValidationOutcome:
  case Valid(event: TransactionEvent)
  case InvalidButPass(event: TransactionEvent, errorCode: String, metricKey: String, reason: String)
  case DeadLetter(rawPayload: String, errorCode: String, metricKey: String, reason: String)

/** Side-channel alerts raised by stateful / windowed checks. */
enum StatefulAlert:
  case TemporalAnomaly(reason: String, metricKey: String, idleForMs: Long)
end StatefulAlert
