package com.scattersphere.tasks

import com.scattersphere.core.util.RunnableTask
import com.typesafe.scalalogging.LazyLogging

class ShellTask(command: String) extends RunnableTask with LazyLogging {

  override def run(): Unit = {
    logger.info(s"Running command ${command}")
  }

  override def onFinished(): Unit = {
    logger.info("Command finished.")
  }

  override def onException(t: Throwable): Unit = {
    logger.info(s"Exception occurred during run: ${t}")
  }

}
