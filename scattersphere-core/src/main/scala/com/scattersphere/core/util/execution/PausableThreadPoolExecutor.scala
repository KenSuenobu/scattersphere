/**
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.scattersphere.core.util.execution

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.locks.{Condition, ReentrantLock}

/**
  * A light wrapper around the [[ThreadPoolExecutor]]. It allows for you to pause execution and
  * resume execution when ready. It is very handy for games that need to pause.
  *
  * (Please note, no license was specified when copied from GitHubGist, so this applies to the LICENSE-2.0
  * as outlined in the start of this code.)
  *
  * @author Matthew A. Johnston (warmwaffles)
  * @author Kenji Suenobu (KenSuenobu)
  * @param corePoolSize    The size of the pool
  * @param maximumPoolSize The maximum size of the pool
  * @param keepAliveTime   The amount of time you wish to keep a single task alive
  * @param unit            The unit of time that the keep alive time represents
  * @param workQueue       The queue that holds your tasks
  * @see ThreadPoolExecutor#ThreadPoolExecutor(int, int, long, TimeUnit, BlockingQueue)
  */
class PausableThreadPoolExecutor(val corePoolSize: Int = Runtime.getRuntime.availableProcessors(),
                                 val maximumPoolSize: Int = Runtime.getRuntime.availableProcessors() * 10,
                                 val keepAliveTime: Long = Long.MaxValue,
                                 val unit: TimeUnit = TimeUnit.SECONDS,
                                 val workQueue: BlockingQueue[Runnable] = new LinkedBlockingQueue[Runnable]())
  extends ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue) {

  private val lock: ReentrantLock = new ReentrantLock()
  private val condition: Condition = lock.newCondition()
  private var paused = false

  /**
    * @param thread   The thread being executed
    * @param runnable The runnable task
    * @see { @link ThreadPoolExecutor#beforeExecute(Thread, Runnable)}
    */
  override protected def beforeExecute(thread: Thread, runnable: Runnable): Unit = {
    super.beforeExecute(thread, runnable)

    lock.lock()

    try {
      while (paused) {
        println("Awaiting lock release.")
        condition.await
        println("Lock released.")
      }
    } catch {
      case _: InterruptedException => thread.interrupt()
    } finally {
      lock.unlock()
    }
  }

  def isRunning: Boolean = !paused

  def isPaused: Boolean = paused

  /**
    * Pause the execution
    */
  def pause(): Unit = {
    println(s"PausableThreadPoolExecutor: Pausing.")
    lock.lock()
    try {
      paused = true
    } finally {
      lock.unlock()
    }
  }

  /**
    * Resume pool execution
    */
  def resume(): Unit = {
    println(s"PausableThreadPoolExecutor: Resuming.")
    lock.lock()

    try {
      paused = false
      condition.signalAll
    } finally {
      lock.unlock()
    }
  }
}

object PausableThreadPoolExecutor {
  def apply(): PausableThreadPoolExecutor = new PausableThreadPoolExecutor()
}
