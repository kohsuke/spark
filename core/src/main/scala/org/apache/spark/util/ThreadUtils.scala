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

package org.apache.spark.util

import java.util
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{Awaitable, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.higherKinds
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.SparkException
import org.apache.spark.rpc.RpcAbortException

private[spark] object ThreadUtils {

  object MDCAwareThreadPoolExecutor {
    def newCachedThreadPool(threadFactory: ThreadFactory): ThreadPoolExecutor = {
      // The values needs to be synced with `Executors.newCachedThreadPool`
      new MDCAwareThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        60L,
        TimeUnit.SECONDS,
        new SynchronousQueue[Runnable],
        threadFactory)
    }

    def newFixedThreadPool(nThreads: Int, threadFactory: ThreadFactory): ThreadPoolExecutor = {
      // The values needs to be synced with `Executors.newFixedThreadPool`
      new MDCAwareThreadPoolExecutor(
        nThreads,
        nThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue[Runnable],
        threadFactory)
    }

    /**
     * This method differ from the [[java.util.concurrent.Executors#newSingleThreadExecutor]] in
     * 2 ways:
     *   1. It use [[org.apache.spark.util.ThreadUtils.MDCAwareThreadPoolExecutor]]
     *   as underline [[java.util.concurrent.ExecutorService]]
     *   2. It does not use the
     *   [[java.util.concurrent.Executors#FinalizableDelegatedExecutorService]] from JDK
     */
    def newSingleThreadExecutor(threadFactory: ThreadFactory): ExecutorService = {
      // The values needs to be synced with `Executors.newSingleThreadExecutor`
      Executors.unconfigurableExecutorService(
        new MDCAwareThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue[Runnable],
          threadFactory)
        )
    }

  }

  class MDCAwareRunnable(proxy: Runnable) extends Runnable {
    val callerThreadMDC: util.Map[String, String] = getMDCMap

    @inline
    private def getMDCMap: util.Map[String, String] = {
      org.slf4j.MDC.getCopyOfContextMap match {
        case null => new util.HashMap[String, String]()
        case m => m
      }
    }

    override def run(): Unit = {
      val threadMDC = getMDCMap
      org.slf4j.MDC.setContextMap(callerThreadMDC)
      try {
        proxy.run()
      } finally {
        org.slf4j.MDC.setContextMap(threadMDC)
      }
    }
  }

  class MDCAwareScheduledThreadPoolExecutor(
      corePoolSize: Int,
      threadFactory: ThreadFactory)
    extends ScheduledThreadPoolExecutor(corePoolSize, threadFactory) {

    override def execute(runnable: Runnable) {
      super.execute(new MDCAwareRunnable(runnable))
    }
  }

  class MDCAwareThreadPoolExecutor(
      corePoolSize: Int,
      maximumPoolSize: Int,
      keepAliveTime: Long,
      unit: TimeUnit,
      workQueue: BlockingQueue[Runnable],
      threadFactory: ThreadFactory)
    extends ThreadPoolExecutor(corePoolSize, maximumPoolSize,
      keepAliveTime, unit, workQueue, threadFactory) {

    override def execute(runnable: Runnable) {
      super.execute(new MDCAwareRunnable(runnable))
    }
  }

  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(sameThreadExecutorService())

  // Inspired by Guava MoreExecutors.sameThreadExecutor; inlined and converted
  // to Scala here to avoid Guava version issues
  def sameThreadExecutorService(): ExecutorService = new AbstractExecutorService {
    private val lock = new ReentrantLock()
    private val termination = lock.newCondition()
    private var runningTasks = 0
    private var serviceIsShutdown = false

    override def shutdown(): Unit = {
      lock.lock()
      try {
        serviceIsShutdown = true
      } finally {
        lock.unlock()
      }
    }

    override def shutdownNow(): java.util.List[Runnable] = {
      shutdown()
      java.util.Collections.emptyList()
    }

    override def isShutdown: Boolean = {
      lock.lock()
      try {
        serviceIsShutdown
      } finally {
        lock.unlock()
      }
    }

    override def isTerminated: Boolean = synchronized {
      lock.lock()
      try {
        serviceIsShutdown && runningTasks == 0
      } finally {
        lock.unlock()
      }
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
      var nanos = unit.toNanos(timeout)
      lock.lock()
      try {
        while (nanos > 0 && !isTerminated()) {
          nanos = termination.awaitNanos(nanos)
        }
        isTerminated()
      } finally {
        lock.unlock()
      }
    }

    override def execute(command: Runnable): Unit = {
      lock.lock()
      try {
        if (isShutdown()) throw new RejectedExecutionException("Executor already shutdown")
        runningTasks += 1
      } finally {
        lock.unlock()
      }
      try {
        command.run()
      } finally {
        lock.lock()
        try {
          runningTasks -= 1
          if (isTerminated()) termination.signalAll()
        } finally {
          lock.unlock()
        }
      }
    }
  }

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
   * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
   */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    MDCAwareThreadPoolExecutor.newCachedThreadPool(threadFactory)
  }

  /**
   * Wrapper over newCachedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newCachedThreadPool(threadFactory: ThreadFactory): ThreadPoolExecutor = {
    MDCAwareThreadPoolExecutor.newCachedThreadPool(threadFactory)
  }

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new MDCAwareThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
   * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
   * unique, sequentially assigned integer.
   */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    MDCAwareThreadPoolExecutor.newFixedThreadPool(nThreads, threadFactory)
  }

  /**
   * Wrapper over newSingleThreadExecutor.
   */
  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    MDCAwareThreadPoolExecutor.newSingleThreadExecutor(threadFactory)
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new MDCAwareScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * Wrapper over ScheduledThreadPoolExecutor.
   */
  def newDaemonThreadPoolScheduledExecutor(threadNamePrefix: String, numThreads: Int)
      : ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat(s"$threadNamePrefix-%d")
      .build()
    val executor = new MDCAwareScheduledThreadPoolExecutor(numThreads, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
   * Run a piece of code in a new thread and return the result. Exception in the new thread is
   * thrown in the caller thread with an adjusted stack trace that removes references to this
   * method for clarity. The exception stack traces will be like the following
   *
   * SomeException: exception-message
   *   at CallerClass.body-method (sourcefile.scala)
   *   at ... run in separate thread using org.apache.spark.util.ThreadUtils ... ()
   *   at CallerClass.caller-method (sourcefile.scala)
   *   ...
   */
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()

    exception match {
      case Some(realException) =>
        // Remove the part of the stack that shows method calls into this helper method
        // This means drop everything from the top until the stack element
        // ThreadUtils.runInNewThread(), and then drop that as well (hence the `drop(1)`).
        val baseStackTrace = Thread.currentThread().getStackTrace().dropWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName)).drop(1)

        // Remove the part of the new thread stack that shows methods call from this helper method
        val extraStackTrace = realException.getStackTrace.takeWhile(
          ! _.getClassName.contains(this.getClass.getSimpleName))

        // Combine the two stack traces, with a place holder just specifying that there
        // was a helper method used, without any further details of the helper
        val placeHolderStackElem = new StackTraceElement(
          s"... run in separate thread using ${ThreadUtils.getClass.getName.stripSuffix("$")} ..",
          " ", "", -1)
        val finalStackTrace = extraStackTrace ++ Seq(placeHolderStackElem) ++ baseStackTrace

        // Update the stack trace and rethrow the exception in the caller thread
        realException.setStackTrace(finalStackTrace)
        throw realException
      case None =>
        result
    }
  }

  /**
   * Construct a new ForkJoinPool with a specified max parallelism and name prefix.
   */
  def newForkJoinPool(prefix: String, maxThreadNumber: Int): ForkJoinPool = {
    // Custom factory to set thread names
    val factory = new ForkJoinPool.ForkJoinWorkerThreadFactory {
      override def newThread(pool: ForkJoinPool) =
        new ForkJoinWorkerThread(pool) {
          setName(prefix + "-" + super.getName)
        }
    }
    new ForkJoinPool(maxThreadNumber, factory,
      null, // handler
      false // asyncMode
    )
  }

  // scalastyle:off awaitresult
  /**
   * Preferred alternative to `Await.result()`.
   *
   * This method wraps and re-throws any exceptions thrown by the underlying `Await` call, ensuring
   * that this thread's stack trace appears in logs.
   *
   * In addition, it calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s
   * `BlockingContext`. Codes running in the user's thread may be in a thread of Scala ForkJoinPool.
   * As concurrent executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this
   * method basically prevents ForkJoinPool from running other tasks in the current waiting thread.
   * In general, we should use this method because many places in Spark use [[ThreadLocal]] and it's
   * hard to debug when [[ThreadLocal]]s leak to other tasks.
   */
  @throws(classOf[SparkException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      case e: SparkFatalException =>
        throw e.throwable
      // TimeoutException and RpcAbortException is thrown in the current thread, so not need to warp
      // the exception.
      case NonFatal(t)
          if !t.isInstanceOf[TimeoutException] && !t.isInstanceOf[RpcAbortException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }
  }
  // scalastyle:on awaitresult

  // scalastyle:off awaitready
  /**
   * Preferred alternative to `Await.ready()`.
   *
   * @see [[awaitResult]]
   */
  @throws(classOf[SparkException])
  def awaitReady[T](awaitable: Awaitable[T], atMost: Duration): awaitable.type = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      // See SPARK-13747.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.ready(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new SparkException("Exception thrown in awaitResult: ", t)
    }
  }
  // scalastyle:on awaitready

  def shutdown(
      executor: ExecutorService,
      gracePeriod: Duration = FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    executor.shutdown()
    executor.awaitTermination(gracePeriod.toMillis, TimeUnit.MILLISECONDS)
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
  }

  /**
   * Transforms input collection by applying the given function to each element in parallel fashion.
   * Comparing to the map() method of Scala parallel collections, this method can be interrupted
   * at any time. This is useful on canceling of task execution, for example.
   *
   * @param in - the input collection which should be transformed in parallel.
   * @param prefix - the prefix assigned to the underlying thread pool.
   * @param maxThreads - maximum number of thread can be created during execution.
   * @param f - the lambda function will be applied to each element of `in`.
   * @tparam I - the type of elements in the input collection.
   * @tparam O - the type of elements in resulted collection.
   * @return new collection in which each element was given from the input collection `in` by
   *         applying the lambda function `f`.
   */
  def parmap[I, O](in: Seq[I], prefix: String, maxThreads: Int)(f: I => O): Seq[O] = {
    val pool = newForkJoinPool(prefix, maxThreads)
    try {
      implicit val ec = ExecutionContext.fromExecutor(pool)

      val futures = in.map(x => Future(f(x)))
      val futureSeq = Future.sequence(futures)

      awaitResult(futureSeq, Duration.Inf)
    } finally {
      pool.shutdownNow()
    }
  }
}
