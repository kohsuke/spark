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

package org.apache.spark.unsafe.types;

public class IntervalConstants {

  public static final Byte DAYS_PER_MONTH = 30;
  public static final Byte MONTHS_PER_QUARTER = 3;

  public static final int MONTHS_PER_YEAR = 12;
  public static final int YEARS_PER_MILLENNIUM = 1000;
  public static final int YEARS_PER_CENTURY = 100;
  public static final int YEARS_PER_DECADE = 10;

  public static final long NANOS_PER_MICRO = 1000L;
  public static final long MILLIS_PER_SECOND = 1000L;
  public static final long MICROS_PER_MILLI = 1000L;
  public static final long SECONDS_PER_DAY = 24L * 60 * 60;

  public static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
  public static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
  public static final long MILLIS_PER_DAY = SECONDS_PER_DAY * MILLIS_PER_SECOND;

  public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLI;
  public static final long MICROS_PER_MINUTE = MILLIS_PER_MINUTE * MICROS_PER_MILLI;
  public static final long MICROS_PER_HOUR = MILLIS_PER_HOUR * MICROS_PER_MILLI;
  public static final long MICROS_PER_DAY = SECONDS_PER_DAY * MICROS_PER_SECOND;
  public static final long MICROS_PER_WEEK = MICROS_PER_DAY * 7;
  public static final long MICROS_PER_MONTH = DAYS_PER_MONTH * MICROS_PER_DAY;
  /* 365.25 days per year assumes leap year every four years */
  public static final long MICROS_PER_YEAR = (36525L * MICROS_PER_DAY) / 100;

  public static final long NANOS_PER_SECOND = NANOS_PER_MICRO * MICROS_PER_SECOND;
  public static final long NANOS_PER_MILLIS = NANOS_PER_MICRO * MICROS_PER_MILLI;

}
