/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.util

import java.time._
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.{ChronoField, TemporalAccessor}
import java.util.Locale

import com.google.common.cache.CacheBuilder

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.catalyst.util.DateTimeFormatterHelper._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy._

trait DateTimeFormatterHelper {
  private def getOrDefault(accessor: TemporalAccessor, field: ChronoField, default: Int): Int = {
    if (accessor.isSupported(field)) {
      accessor.get(field)
    } else {
      default
    }
  }

  protected def toLocalDate(accessor: TemporalAccessor, allowMissingYear: Boolean): LocalDate = {
    val year = if (accessor.isSupported(ChronoField.YEAR)) {
      accessor.get(ChronoField.YEAR)
    } else if (allowMissingYear) {
      // To keep backward compatibility with Spark 2.x, we pick 1970 as the default value of year.
      1970
    } else {
      throw new SparkUpgradeException("3.0",
        "Year must be given in the date/timestamp string to be parsed. You can set " +
          SQLConf.LEGACY_ALLOW_MISSING_YEAR_DURING_PARSING.key + " to true, to pick 1970 as " +
          "the default value of year.", null)
    }
    val month = getOrDefault(accessor, ChronoField.MONTH_OF_YEAR, 1)
    val day = getOrDefault(accessor, ChronoField.DAY_OF_MONTH, 1)
    LocalDate.of(year, month, day)
  }

  // Converts the parsed temporal object to ZonedDateTime. It sets time components to zeros
  // if they does not exist in the parsed object.
  protected def toZonedDateTime(
      temporalAccessor: TemporalAccessor,
      zoneId: ZoneId,
      allowMissingYear: Boolean): ZonedDateTime = {
    val hour = if (temporalAccessor.isSupported(ChronoField.HOUR_OF_DAY)) {
      temporalAccessor.get(ChronoField.HOUR_OF_DAY)
    } else if (temporalAccessor.isSupported(ChronoField.HOUR_OF_AMPM)) {
      // When we reach here, it means am/pm is not specified. Here we assume it's am.
      temporalAccessor.get(ChronoField.HOUR_OF_AMPM)
    } else {
      0
    }
    val minute = getOrDefault(temporalAccessor, ChronoField.MINUTE_OF_HOUR, 0)
    val second = getOrDefault(temporalAccessor, ChronoField.SECOND_OF_MINUTE, 0)
    val nanoSecond = getOrDefault(temporalAccessor, ChronoField.NANO_OF_SECOND, 0)
    val localTime = LocalTime.of(hour, minute, second, nanoSecond)
    val localDate = toLocalDate(temporalAccessor, allowMissingYear)
    ZonedDateTime.of(localDate, localTime, zoneId)
  }

  // Gets a formatter from the cache or creates new one. The buildFormatter method can be called
  // a few times with the same parameters in parallel if the cache does not contain values
  // associated to those parameters. Since the formatter is immutable, it does not matter.
  // In this way, synchronised is intentionally omitted in this method to make parallel calls
  // less synchronised.
  // The Cache.get method is not used here to avoid creation of additional instances of Callable.
  protected def getOrCreateFormatter(
      pattern: String,
      locale: Locale,
      needVarLengthSecondFraction: Boolean = false): DateTimeFormatter = {
    val newPattern = convertIncompatiblePattern(pattern)
    val useVarLen = needVarLengthSecondFraction && newPattern.contains('S')
    val key = (newPattern, locale, useVarLen)
    var formatter = cache.getIfPresent(key)
    if (formatter == null) {
      formatter = buildFormatter(newPattern, locale, useVarLen)
      cache.put(key, formatter)
    }
    formatter
  }

  // When legacy time parser policy set to EXCEPTION, check whether we will get different results
  // between legacy parser and new parser. If new parser fails but legacy parser works, throw a
  // SparkUpgradeException. On the contrary, if the legacy policy set to CORRECTED,
  // DateTimeParseException will address by the caller side.
  protected def checkDiffResult[T](
      s: String, legacyParseFunc: String => T): PartialFunction[Throwable, T] = {
    case e: DateTimeException if SQLConf.get.legacyTimeParserPolicy == EXCEPTION =>
      try {
        legacyParseFunc(s)
        throw new SparkUpgradeException("3.0", s"Fail to parse '$s' in the new parser. You can " +
          s"set ${SQLConf.LEGACY_TIME_PARSER_POLICY.key} to LEGACY to restore the behavior " +
          s"before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.", e)
      } catch {
        case _: Throwable => throw e
      }
  }
}

private object DateTimeFormatterHelper {
  val cache = CacheBuilder.newBuilder()
    .maximumSize(128)
    .build[(String, Locale, Boolean), DateTimeFormatter]()

  final val extractor = "^([^S]*)(S*)(.*)$".r

  def createBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  def toFormatter(builder: DateTimeFormatterBuilder, locale: Locale): DateTimeFormatter = {
    builder
      .toFormatter(locale)
      .withChronology(IsoChronology.INSTANCE)
      .withResolverStyle(ResolverStyle.STRICT)
  }

  /**
   * Building a formatter for parsing seconds fraction with variable length
   */
  def createBuilderWithVarLengthSecondFraction(
      pattern: String): DateTimeFormatterBuilder = {
    val builder = createBuilder()
    pattern.split("'").zipWithIndex.foreach {
      // Split string starting with the regex itself which is `'` here will produce an extra empty
      // string at res(0). So when the first element here is empty string we do not need append `'`
      // literal to the DateTimeFormatterBuilder.
      case ("", idx) if idx != 0 => builder.appendLiteral("'")
      case (pattenPart, idx) if idx % 2 == 0 =>
        var rest = pattenPart
        while (rest.nonEmpty) {
          rest match {
            case extractor(prefix, secondFraction, suffix) =>
              builder.appendPattern(prefix)
              if (secondFraction.nonEmpty) {
                builder.appendFraction(ChronoField.NANO_OF_SECOND, 1, secondFraction.length, false)
              }
              rest = suffix
            case _ => throw new IllegalArgumentException(s"Unrecognized datetime pattern: $pattern")
          }
        }
      case (patternPart, _) => builder.appendLiteral(patternPart)
    }
    builder
  }

  def buildFormatter(
      pattern: String,
      locale: Locale,
      varLenEnabled: Boolean): DateTimeFormatter = {
    val builder = if (varLenEnabled) {
      createBuilderWithVarLengthSecondFraction(pattern)
    } else {
      createBuilder().appendPattern(pattern)
    }
    toFormatter(builder, locale)
  }

  lazy val fractionFormatter: DateTimeFormatter = {
    val builder = createBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    toFormatter(builder, TimestampFormatter.defaultLocale)
  }

  final val unsupportedLetters = Set('A', 'c', 'e', 'n', 'N', 'p')

  /**
   * In Spark 3.0, we switch to the Proleptic Gregorian calendar and use DateTimeFormatter for
   * parsing/formatting datetime values. The pattern string is incompatible with the one defined
   * by SimpleDateFormat in Spark 2.4 and earlier. This function converts all incompatible pattern
   * for the new parser in Spark 3.0. See more details in SPARK-31030.
   * @param pattern The input pattern.
   * @return The pattern for new parser
   */
  def convertIncompatiblePattern(pattern: String): String = {
    val eraDesignatorContained = pattern.split("'").zipWithIndex.exists {
      case (patternPart, index) =>
        // Text can be quoted using single quotes, we only check the non-quote parts.
        index % 2 == 0 && patternPart.contains("G")
    }
    (pattern + " ").split("'").zipWithIndex.map {
      case (patternPart, index) =>
        if (index % 2 == 0) {
          for (c <- patternPart if unsupportedLetters.contains(c)) {
            throw new IllegalArgumentException(s"Illegal pattern character: $c")
          }
          // The meaning of 'u' was day number of week in SimpleDateFormat, it was changed to year
          // in DateTimeFormatter. Substitute 'u' to 'e' and use DateTimeFormatter to parse the
          // string. If parsable, return the result; otherwise, fall back to 'u', and then use the
          // legacy SimpleDateFormat parser to parse. When it is successfully parsed, throw an
          // exception and ask users to change the pattern strings or turn on the legacy mode;
          // otherwise, return NULL as what Spark 2.4 does.
          val res = patternPart.replace("u", "e")
          // In DateTimeFormatter, 'u' supports negative years. We substitute 'y' to 'u' here for
          // keeping the support in Spark 3.0. If parse failed in Spark 3.0, fall back to 'y'.
          // We only do this substitution when there is no era designator found in the pattern.
          if (!eraDesignatorContained) {
            res.replace("y", "u")
          } else {
            res
          }
        } else {
          patternPart
        }
    }.mkString("'").stripSuffix(" ")
  }
}
