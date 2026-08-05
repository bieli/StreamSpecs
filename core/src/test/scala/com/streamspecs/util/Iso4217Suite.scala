package com.streamspecs.util

import munit.FunSuite

class Iso4217Suite extends FunSuite:
  test("recognizes common currencies") {
    assert(Iso4217.isValid("PLN"))
    assert(Iso4217.isValid("EUR"))
    assert(Iso4217.isValid(" usd "))
  }

  test("rejects crypto tickers and junk") {
    assert(!Iso4217.isValid("BTC"))
    assert(!Iso4217.isValid("XX"))
    assert(!Iso4217.isValid("EURO"))
    assert(!Iso4217.isValid(""))
  }

  test("isAlpha3Format is shape-only") {
    assert(Iso4217.isAlpha3Format("BTC"))
    assert(Iso4217.isAlpha3Format("eur"))
    assert(!Iso4217.isAlpha3Format("EU"))
  }

  test("toCurrency returns JVM Currency for valid codes") {
    assert(Iso4217.toCurrency("EUR").isDefined)
    assertEquals(Iso4217.toCurrency("BTC"), None)
  }
end Iso4217Suite
