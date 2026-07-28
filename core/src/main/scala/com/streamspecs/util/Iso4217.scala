package com.streamspecs.util

import java.util.Currency
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Optional ISO 4217 helper for domains that need alphabetic currency codes. */
object Iso4217:
  val codes: Set[String] =
    Currency.getAvailableCurrencies.asScala.map(_.getCurrencyCode.toUpperCase).toSet

  private val Alpha3 = "^[A-Za-z]{3}$".r

  def isValid(code: String): Boolean =
    val normalized = code.trim.toUpperCase
    Alpha3.matches(normalized) && codes.contains(normalized)

  def isAlpha3Format(code: String): Boolean =
    Alpha3.matches(code.trim)

  def toCurrency(code: String): Option[Currency] =
    val normalized = code.trim.toUpperCase
    Option.when(isValid(normalized))(Try(Currency.getInstance(normalized)).toOption).flatten
end Iso4217
