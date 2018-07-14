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

import io.buildfactory.scattersphere.core.util.RunnableTask
import io.buildfactory.scattersphere.core.util.logging.SimpleLogger

import scala.sys.process.{Process, ProcessBuilder}

/** [[RunnableTask]] that runs a docker process with a container name, and the given command, in --net host mode
  * ("--net host") in docker.  These flags can be overridden, but it is not recommended that this be done unless you know
  * what you're doing!
  *
  * @param containerName name of the container to use
  * @param command the command to run from the referenced container
  */
class DockerTask(containerName: String, command: String) extends RunnableTask with SimpleLogger {

  private var processBuilder: ProcessBuilder = null
  private var process: Process = null
  private var dockerFlags: String = "--net host"

  override def run(): Unit = {
    logger.debug(s"Running command ${command}")
    processBuilder = Process(s"docker run $dockerFlags $containerName $command")
    process = processBuilder.run
  }

  /** Retrieves the current docker flags.
    *
    * @return docker flags
    */
  def getDockerFlags(): String = dockerFlags

  /** Sets the docker flags to use - this will override the default "--net host" flags, so if you still need to use them,
    * make sure to include them in the flags statement.
    *
    * @param flags the flags to send to Docker.
    */
  def setDockerFlags(flags: String): Unit = dockerFlags = flags

  /** Returns the output (stdout) from the process as a stream of [[String]] data.
    *
    * @return stream of string objects
    */
  def getProcessOutput(): Stream[String] = processBuilder.lineStream

  override def onFinished(): Unit = {
    logger.trace(s"Command finished: ${command}")

    logger.info("Process is still running; terminating.")
    process.destroy()

    logger.debug(s"Command exit code: ${process.exitValue()}")
  }

  override def onException(t: Throwable): Unit = {
    logger.debug("Exception occurred during run", t)

    logger.debug("Process is still running; terminating.")
    process.destroy()
  }


}
