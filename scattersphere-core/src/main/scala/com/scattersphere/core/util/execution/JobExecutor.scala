package com.scattersphere.core.util.execution

import java.util.concurrent.CompletableFuture

import com.scattersphere.core.util.{JobDesc, RunnableTaskStatus, TaskDesc}
import org.slf4j.{Logger, LoggerFactory}

class JobExecutor(engine: ExecutionEngine,
                  job: JobDesc) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def start(): CompletableFuture[Void] = {
    logger.info(s"Starting for engine $engine job $job")

    val runnableTasks = createExecutionPlan

    logger.info("Setting all tasks to WAITING")
    job.tasks.foreach(task => setAllTaskStatus(task, RunnableTaskStatus.WAITING))

    null
  }

  private def createExecutionPlan: Map[String, Executable] = {
    Map()
  }

  private def setAllTaskStatus(task: TaskDesc, status: RunnableTaskStatus.Value): Unit = {
    task.getDependencies.foreach(subtask => {
      logger.info(s"Set subtask $subtask to status $status")

      subtask.executableTask.setStatus(status)

      if (subtask.getDependencies.length > 0) {
        setAllTaskStatus(subtask, status)
      }
    })

    logger.info(s"Set task $task to status $status")

    task.executableTask.setStatus(status)
  }

}

class Executable