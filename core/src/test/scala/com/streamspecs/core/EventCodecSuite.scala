package com.streamspecs.core

import io.circe.Codec
import munit.FunSuite

class EventCodecSuite extends FunSuite:

  final case class JsonEvt(name: String, n: Int) derives Codec.AsObject

  private val codec = EventCodec.fromCirce[JsonEvt]

  test("fromCirce round-trips JSON") {
    val raw = codec.encode(JsonEvt("sensor", 7))
    assertEquals(codec.decode(raw), Right(JsonEvt("sensor", 7)))
  }

  test("fromCirce maps parse errors to Left") {
    assert(codec.decode("{not-json").isLeft)
    assert(codec.decode("""{"name":1}""").isLeft)
  }

  test("SampleEvent csv codec rejects partial rows") {
    import SampleEvent.given
    assert(EventCodec[SampleEvent].decode("only-id").isLeft)
    assert(EventCodec[SampleEvent].decode("a,not-a-number,1").isLeft)
    assertEquals(EventCodec[SampleEvent].decode("a,1.5,9"), Right(SampleEvent("a", 1.5, 9)))
  }
end EventCodecSuite
