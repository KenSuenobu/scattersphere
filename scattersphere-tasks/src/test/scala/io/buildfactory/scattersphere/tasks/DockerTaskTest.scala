/*
 *    _____            __  __                       __
 *   / ___/_________ _/ /_/ /____  ______________  / /_  ___  ________
 *   \__ \/ ___/ __ `/ __/ __/ _ \/ ___/ ___/ __ \/ __ \/ _ \/ ___/ _ \
 *  ___/ / /__/ /_/ / /_/ /_/  __/ /  (__  ) /_/ / / / /  __/ /  /  __/
 * /____/\___/\__,_/\__/\__/\___/_/  /____/ .___/_/ /_/\___/_/   \___/
 *                                       /_/
 *
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
import io.buildfactory.scattersphere.core.util.{Job, JobBuilder, Task, TaskBuilder}
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger
import org.scalatest.{FlatSpec, Matchers}

class DockerTaskTest extends FlatSpec with Matchers with SimpleLogger {

  "Docker task" should "be able to get ubuntu and run a command within its container" in {
    val dockerTask: DockerTask = new DockerTask("centos", "/bin/ls -al /etc/")
    val sTask1: Task = TaskBuilder("DockerTask1").withTask(dockerTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    job.status shouldBe JobFinished

    val output: List[String] = dockerTask.getProcessOutput().toList

    assert(output.size > 2)
    assert(output(0).toLowerCase() contains "total")
  }

  it should "be able to run ps -axwww" in {
    val dockerTask: DockerTask = new DockerTask("centos", "/bin/ps -axwww")
    val sTask1: Task = TaskBuilder("DockerTaskPs").withTask(dockerTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    job.status shouldBe JobFinished

    val output: List[String] = dockerTask.getProcessOutput().toList

    assert(output.size > 2)
    assert(output(0).toLowerCase() contains "ps -axwww")
  }

  it should "be able to run curl with --net host flags" in {
    val dockerTask: DockerTask = new DockerTask("centos", "curl http://www.google.com/")

    dockerTask.setDockerFlags("-it --net host")
    dockerTask.getDockerFlags() shouldBe "-it --net host"

    val sTask1: Task = TaskBuilder("centosTask3").withTask(dockerTask).build()
    val job: Job = JobBuilder().withTasks(sTask1).build()
    val jExec: JobExecutor = JobExecutor(job)

    jExec.queue().run()
    job.status shouldBe JobFinished

    val output: List[String] = dockerTask.getProcessOutput().toList

    assert(output.size > 2)
  }

}
