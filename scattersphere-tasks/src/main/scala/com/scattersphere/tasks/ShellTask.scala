package com.scattersphere.tasks

import com.scattersphere.core.util.RunnableTask
import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.{Process, ProcessBuilder}

/** Executes a Shell command on the command line.  These commands must terminate at some point.  There is no timeout
  * delay for the task, so if a failure happens on the Shell task and it hangs, the underlying job controller must
  * terminate the task manually.
  *
  * @constructor creates a new ShellTask with the command to execute
  * @param command command string to execute, options all separated by spaces
  * @since 0.1.0
  */
class ShellTask(command: String) extends RunnableTask with LazyLogging {

  private var processBuilder: ProcessBuilder = null
  private var process: Process = null

  override def run(): Unit = {
    logger.info(s"Running command ${command}")
    processBuilder = Process(command)
    process = processBuilder.run
  }

  /** Returns the output (stdout) from the process as a stream of [[String]] data.
    *
    * @return stream of string objects
    */
  def getProcessOutput(): Stream[String] = processBuilder.lineStream

  override def onFinished(): Unit = {
    logger.info("Command finished.")

    if (process.isAlive()) {
      logger.info("Process is still running; terminating.")
      process.destroy()
    }

    logger.info(s"Command exit code: ${process.exitValue()}")
  }

  override def onException(t: Throwable): Unit = {
    logger.info("Exception occurred during run", t)

    if (process.isAlive()) {
      logger.info("Process is still running; terminating.")
      process.destroy()
    }
  }

}

object ShellTask {
  def apply(command: String): ShellTask = new ShellTask(command)
}
