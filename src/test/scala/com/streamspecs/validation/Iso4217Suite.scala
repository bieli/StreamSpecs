package com.streamspecs

import com.streamspecs.validation.Iso4217
import munit.FunSuite

class Iso4217Suite extends FunSuite:

  test("recognizes common banking currencies") {
    List("PLN", "EUR", "USD", "GBP", "CHF", "JPY", "CZK").foreach { code =>
      assert(Iso4217.isValid(code), s"$code should be ISO 4217")
    }
  }

  test("rejects crypto and malformed codes") {
    List("BTC", "ETH", "XBT", "EURO", "PL", "123", "").foreach { code =>
      assert(!Iso4217.isValid(code), s"$code should not be ISO 4217")
    }
  }

  test("alpha-3 format is case-insensitive shape check") {
    assert(Iso4217.isAlpha3Format("pln"))
    assert(Iso4217.isAlpha3Format("USD"))
    assert(!Iso4217.isAlpha3Format("US"))
    assert(!Iso4217.isAlpha3Format("USDT"))
  }

  test("toCurrency resolves JDK instances for valid codes") {
    assert(Iso4217.toCurrency("EUR").isDefined)
    assert(Iso4217.toCurrency("BTC").isEmpty)
  }
end Iso4217Suite
