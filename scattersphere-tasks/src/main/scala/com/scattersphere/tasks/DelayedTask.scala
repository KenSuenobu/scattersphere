package com.scattersphere.tasks

import com.scattersphere.core.util.{RunnableTask, TaskStatus}
import com.typesafe.scalalogging.LazyLogging

class DelayedTask(delay: Int,
                  task: RunnableTask) extends RunnableTask with LazyLogging {

  override def run(): Unit = {
    logger.info(s"Sleeping ${delay}ms before running task.")
    Thread.sleep(delay)
    task.run()
  }

  override def onFinished(): Unit = task.onFinished()

  override def onException(t: Throwable): Unit = task.onException(t)

  override def onStatusChange(old: TaskStatus, current: TaskStatus): Unit = task.onStatusChange(old, current)

}
