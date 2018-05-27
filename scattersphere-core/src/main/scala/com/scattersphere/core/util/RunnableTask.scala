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
package com.scattersphere.core.util

import java.io.{PrintWriter, StringWriter}

import com.typesafe.scalalogging.LazyLogging

/** An abstract class that extends Runnable, providing methods for cleanup and exception handling.
  *
  * [[Task]] objects are defined using this class.  If you have already defined a series of
  * Runnable classes and do not wish to inherit these functions by default, you can always use
  * "with RunnableTask", and these functions will automatically be included in your Runnable.
  *
  * ==Examples==
  *
  * RunnableTask:
  * {{{
  *   class MyTask extends RunnableTask {
  *     // RunnableTask extends Runnable, so you must define run()
  *     override def run(): Unit = {
  *       ... do some expensive unit of work here ...
  *     }
  *
  *     override def onFinished: Unit = {
  *       ... clean up code, shared resources, database connections, etc ...
  *     }
  *   }
  * }}}
  *
  * If you do not want to use the RunnableTask, when creating a new [[Task]], simply use the
  * following similar code:
  *
  * {{{
  *   val myTask: RunnableTask = new MyRunnable() with RunnableTask
  *
  *   ... or ...
  *
  *   val myTask: RunnableTask = RunnableTask(new MyRunnable())
  * }}}
  *
  * This way, you get the added benefits of the RunnableTask's functions without having to directly
  * implement them yourself.  Keep in mind, if you wrap your code using the [[RunnableTask]] factory
  * method, you will not be able to access your class methods.
  *
  * @since 0.0.1
  */
trait RunnableTask extends Runnable with LazyLogging {

  /** Called when the run() method finishes without throwing an exception.  Contains a default
    * implementation if not overridden.
    */
  def onFinished(): Unit = {
    logger.info("Job finished.")
  }

  /** Called when the run() method fails to finish due to an uncaught exception.  Default implementation
    * will display the error stack trace to the logger.
    *
    * @param t Throwable containing the error that occurred.
    */
  def onException(t: Throwable): Unit = {
    val sWriter = new StringWriter()
    val pWriter = new PrintWriter(sWriter)

    t.printStackTrace(pWriter)
    logger.info(s"Exception occurred: ${sWriter.toString}")
  }

  /** Indicates a change in [[TaskStatus]] has taken place.
    *
    * @param old the previous status
    * @param current the status being changed to
    * @since 0.1.0
    */
  def onStatusChange(old: TaskStatus, current: TaskStatus): Unit = {
    logger.trace(s"Status change: old=$old current=$current")
  }

}

/** Factory for [[RunnableTask]] instances.
  *
  * Example:
  * {{{
  *   import com.scattersphere.core.util._
  *   val runnableTask: RunnableTask = RunnableTask(new myRunnable())
  * }}}
  *
  * @since 0.0.1
  */
object RunnableTask {

  /** Creates a new RunnableTask, wrapping the applied runnable object.
    *
    * @param runnable the runnable to wrap.
    * @return a RunnableTask object.
    */
  def apply(runnable: Runnable): RunnableTask = new RunnableTask {
    override def run(): Unit = {
      runnable.run()
    }
  }

}