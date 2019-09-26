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

package org.apache.spark.internal.io

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for instantiation of FileCommitProtocol implementations.
 */
class FileCommitProtocolInstantiationSuite extends SparkFunSuite {

  test("Dynamic partitions require appropriate constructor") {

    // you cannot instantiate a two-arg client with dynamic partitions
    // enabled.
    val ex = intercept[IllegalArgumentException] {
      instantiateClassic(true)
    }
    // check the contents of the message and rethrow if unexpected.
    // this preserves the stack trace of the unexpected
    // exception.
    if (!ex.toString.contains("Dynamic Partition Overwrite")) {
      fail(s"Wrong text in caught exception $ex", ex)
    }
  }

  test("Standard partitions work with classic constructor") {
    instantiateClassic(false)
  }

  test("Three arg constructors have priority") {
    assert(3 == instantiateNew(false).argCount,
      "Wrong constructor argument count")
  }

  test("Three arg constructors have priority when dynamic") {
    assert(3 == instantiateNew(true).argCount,
      "Wrong constructor argument count")
  }

  test("Four-arg constructor has priority") {
    assert(4 == instantiateWithFileSourceWriteDesc(false, None).argCount,
      "Wrong constructor argument count")
  }

  test("Four-arg constructor has priority with file source write description specified") {
    assert(4 == instantiateWithFileSourceWriteDesc(false,
      Some(FileSourceWriteDesc(true, Seq.empty))).argCount, "Wrong constructor argument count")
  }

  test("The protocol must be of the correct class") {
    intercept[ClassCastException] {
      FileCommitProtocol.instantiate(
        classOf[Other].getCanonicalName,
        "job",
        "path",
        false)
    }
  }

  test("If there is no matching constructor, class hierarchy is irrelevant") {
    intercept[NoSuchMethodException] {
      FileCommitProtocol.instantiate(
        classOf[NoMatchingArgs].getCanonicalName,
        "job",
        "path",
        false)
    }
  }

  /**
   * Create a classic two-arg protocol instance.
   * @param dynamic dynamic partitioning mode
   * @return the instance
   */
  private def instantiateClassic(dynamic: Boolean): ClassicConstructorCommitProtocol = {
    FileCommitProtocol.instantiate(
      classOf[ClassicConstructorCommitProtocol].getCanonicalName,
      "job",
      "path",
      dynamic).asInstanceOf[ClassicConstructorCommitProtocol]
  }

  /**
   * Create a three-arg protocol instance.
   * @param dynamic dynamic partitioning mode
   * @return the instance
   */
  private def instantiateNew(
    dynamic: Boolean): FullConstructorCommitProtocol = {
    FileCommitProtocol.instantiate(
      classOf[FullConstructorCommitProtocol].getCanonicalName,
      "job",
      "path",
      dynamic).asInstanceOf[FullConstructorCommitProtocol]
  }

  /**
   * Create a four-arg protocol instance.
   * @param dynamic dynamic partitioning mode
   * @param desc file source write description
   * @return the instance
   */
  private def instantiateWithFileSourceWriteDesc(
      dynamic: Boolean, desc: Option[FileSourceWriteDesc]): FullConstructorCommitProtocol = {
    FileCommitProtocol.instantiate(
      classOf[FullConstructorCommitProtocol].getCanonicalName,
      "job",
      "path",
      dynamic,
      None).asInstanceOf[FullConstructorCommitProtocol]
  }
}

/**
 * This protocol implementation does not have the new three-arg
 * constructor.
 */
private class ClassicConstructorCommitProtocol(arg1: String, arg2: String)
  extends HadoopMapReduceCommitProtocol(arg1, arg2) {
}

/**
 * This protocol implementation does have the new three-arg constructor
 * alongside the original, and a 4 arg one for completeness.
 * The final value of the real constructor is the number of arguments
 * used in the 2-, 3- and 4- constructor, for test assertions.
 */
private class FullConstructorCommitProtocol(
  arg1: String,
  arg2: String,
  b: Boolean,
  desc: Option[FileSourceWriteDesc],
  val argCount: Int)
  extends HadoopMapReduceCommitProtocol(arg1, arg2, b) {

  def this(arg1: String, arg2: String) = {
    this(arg1, arg2, false, None, 2)
  }

  def this(arg1: String, arg2: String, b: Boolean) = {
    this(arg1, arg2, false, None, 3)
  }

  def this(arg1: String, arg2: String, b: Boolean, desc: Option[FileSourceWriteDesc]) {
    this(arg1, arg2, false, None, 4)
  }
}

/**
 * This has the 2-arity constructor, but isn't the right class.
 */
private class Other(arg1: String, arg2: String) {

}

/**
 * This has no matching arguments as well as being the wrong class.
 */
private class NoMatchingArgs() {

}

