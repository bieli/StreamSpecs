package com.streamspecs.core

/** Decode / encode domain events from Kafka string payloads (typically JSON). */
trait EventCodec[T]:
  def decode(raw: String): Either[String, T]
  def encode(event: T): String

object EventCodec:
  def apply[T](using c: EventCodec[T]): EventCodec[T] = c

  /** Circe-backed codec for types that already have Encoder/Decoder. */
  def fromCirce[T](using enc: io.circe.Encoder[T], dec: io.circe.Decoder[T]): EventCodec[T] =
    new EventCodec[T]:
      def decode(raw: String): Either[String, T] =
        io.circe.parser.decode[T](raw).left.map(_.getMessage)
      def encode(event: T): String =
        enc(event).noSpaces
end EventCodec
