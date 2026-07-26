package com.streamspecs.validation

import java.util.Currency
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** ISO 4217 alphabetic currency codes (banking / insurance standard).
  *
  * Uses the JDK registry (`java.util.Currency`) so the set tracks the runtime's ISO 4217 table —
  * three-letter codes such as PLN, EUR, USD, GBP, CHF, JPY.
  *
  * Non-ISO tickers (e.g. BTC, ETH) are rejected.
  */
object Iso4217:

  /** Active alphabetic codes known to this JVM (uppercase). */
  val codes: Set[String] =
    Currency.getAvailableCurrencies.asScala.map(_.getCurrencyCode.toUpperCase).toSet

  private val Alpha3 = "^[A-Za-z]{3}$".r

  /** True iff `code` is a 3-letter ISO 4217 alphabetic currency code. */
  def isValid(code: String): Boolean =
    val normalized = code.trim.toUpperCase
    Alpha3.matches(normalized) && codes.contains(normalized)

  /** Format-only check: exactly three ASCII letters (ISO 4217 alphabetic shape). */
  def isAlpha3Format(code: String): Boolean =
    Alpha3.matches(code.trim)

  /** Resolve JDK Currency if the code is ISO 4217; None otherwise. */
  def toCurrency(code: String): Option[Currency] =
    val normalized = code.trim.toUpperCase
    Option
      .when(isValid(normalized))(
        Try(Currency.getInstance(normalized)).toOption
      )
      .flatten
end Iso4217
