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

package io.buildfactory.scattersphere.tasks

import com.typesafe.scalalogging.LazyLogging
import io.buildfactory.scattersphere.core.util.{RunnableTask, TaskStatus}

/** Calls the embedded [[RunnableTask]] after a specified delay.
  *
  * @param delay amount of time to wait before triggering run() in milliseconds
  * @param task the [[RunnableTask]] to run after the delay
  * @since 0.1.0
  */
class DelayedTask(delay: Int,
                  task: RunnableTask) extends RunnableTask with LazyLogging {

  override def run(): Unit = {
    logger.info(s"Sleeping ${delay}ms before running task.")
    Thread.sleep(delay)
    task.run()
  }

  override def onFinished(): Unit = {
    super.onFinished()
    task.onFinished()
  }

  override def onException(t: Throwable): Unit = {
    super.onException(t)
    task.onException(t)
  }

  override def onStatusChange(old: TaskStatus, current: TaskStatus): Unit = {
    super.onStatusChange(old, current)
    task.onStatusChange(old, current)
  }

}
