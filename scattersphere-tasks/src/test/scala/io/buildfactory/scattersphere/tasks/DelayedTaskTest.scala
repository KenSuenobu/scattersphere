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

import io.buildfactory.scattersphere.core.util.execution.JobExecutor
import io.buildfactory.scattersphere.core.util._
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger
import org.scalatest.{FlatSpec, Matchers}

class DelayedTaskTest extends FlatSpec with Matchers with SimpleLogger {

  "Delayed Task" should "delay 5 seconds before running a job" in {
    class SleeperThread extends Runnable {
      override def run(): Unit = {
        println(s"Triggered!")
      }
    }

    var sTask = TaskBuilder("Delayed Task")
      .withTask(new DelayedTask(2000, RunnableTask(new SleeperThread)))
      .build()
    var sJob = JobBuilder()
      .withTasks(sTask)
      .build()
    var jExec: JobExecutor = JobExecutor(sJob)

    jExec.setBlocking(false).queue().run()

    logger.info("Sleeping 1 second before checking job and task status ...")
    Thread.sleep(1000)
    sJob.status shouldBe JobRunning
    sTask.status shouldBe TaskRunning

    logger.info("Allowing task to finish before checking job and task status ...")
    Thread.sleep(2000)
    sJob.status shouldBe JobFinished
    sTask.status shouldBe TaskFinished
  }

}
