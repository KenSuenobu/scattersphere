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

package com.scattersphere.tasks

import java.util.concurrent.atomic.AtomicInteger

import com.scattersphere.core.util.{RunnableTask, TaskStatus}
import com.typesafe.scalalogging.LazyLogging

/** Repeats a [[RunnableTask]] a number of times.
  *
  * @param times number of times to run (must be > 0)
  * @param task the task to repeat
  * @since 0.1.0
  */
class RepeatingTask(times: Int,
                    task: RunnableTask) extends RunnableTask with LazyLogging {

  private val timesRun: AtomicInteger = new AtomicInteger(0)

  override def run(): Unit = {
    if (times <= 0) {
      throw new IllegalArgumentException("Number of times must be > 0")
    }

    while(timesRun.get() < times) {
      task.run()
      task.onFinished()
      timesRun.incrementAndGet()
    }
  }

  /** Retrieves the number of times the [[RunnableTask]] has completed.
    *
    * @return number of times the task has run.
    */
  def getTimesRepeated(): Int = timesRun.get()

  override def onFinished(): Unit = task.onFinished()

  override def onException(t: Throwable): Unit = task.onException(t)

  override def onStatusChange(old: TaskStatus, current: TaskStatus): Unit = task.onStatusChange(old, current)

}
