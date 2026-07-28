package com.streamspecs.util

import munit.FunSuite

class Iso4217Suite extends FunSuite:
  test("recognizes common currencies") {
    assert(Iso4217.isValid("PLN"))
    assert(Iso4217.isValid("EUR"))
  }
  test("rejects crypto tickers") {
    assert(!Iso4217.isValid("BTC"))
  }
end Iso4217Suite
