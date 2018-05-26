package com.scattersphere.tasks

import com.scattersphere.core.util.RunnableTask
import com.typesafe.scalalogging.LazyLogging

import scala.sys.process.{Process, ProcessBuilder}

class ShellTask(command: String) extends RunnableTask with LazyLogging {

  private var processBuilder: ProcessBuilder = null
  private var processStream: Stream[String] = null
  private var process: Process = null

  override def run(): Unit = {
    logger.info(s"Running command ${command}")
    processBuilder = Process(command)
    processStream = processBuilder.lineStream
    process = processBuilder.run
  }

  def getProcessOutput(): Stream[String] = processStream

  override def onFinished(): Unit = {
    logger.info("Command finished.")

    if (process.isAlive()) {
      logger.info("Process is still running; terminating.")
      process.destroy()
    }

    logger.info(s"Command exit code: ${process.exitValue()}")
  }

  override def onException(t: Throwable): Unit = {
    logger.info(s"Exception occurred during run: ${t}")

    if (process.isAlive()) {
      logger.info("Process is still running; terminating.")
      process.destroy()
    }
  }

}

object ShellTask {
  def apply(command: String): ShellTask = new ShellTask(command)
}
