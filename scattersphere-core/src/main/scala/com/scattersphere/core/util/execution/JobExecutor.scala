package com.scattersphere.core.util.execution

import java.util.concurrent.CompletableFuture
import java.util.stream.Collectors

import com.scattersphere.core.util.{JobDesc, RunnableTaskStatus, TaskDesc}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class JobExecutor(engine: ExecutionEngine,
                  job: JobDesc) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def start(): CompletableFuture[Void] = {
    logger.info(s"Starting for engine $engine job $job")

    logger.info("Creating task execution plan")
    val runnableTasks = createExecutionPlan

    logger.info("Setting all tasks to WAITING")
    job.tasks.foreach(task => setAllTaskStatus(task, RunnableTaskStatus.WAITING))

    null
  }

  private def createExecutionPlan: Map[String, Executable] = {
    val plan: mutable.HashMap[String, Executable] = new mutable.HashMap()

    job.tasks.foreach(task => {
      if (plan.getOrElse(task.name, null) != null) {
        throw new DuplicateTaskException(job.name, task.name)
      }

      // Filter out any tasks that contain a dependency with this task's name.
      val dependentTasks = job.tasks.filter(_.getDependencies.contains(task))

      logger.info(s"Adding: task=$task dependentTasks=$dependentTasks")

      plan.put(task.name, new Executable(System.currentTimeMillis(), task, dependentTasks, task.task))
    })

    plan.toMap
  }

  private def setAllTaskStatus(task: TaskDesc, status: RunnableTaskStatus.Value): Unit = {
    task.getDependencies.foreach(subtask => {
      logger.info(s"Set subtask $subtask to status $status")

      subtask.task.setStatus(status)

      if (subtask.getDependencies.length > 0) {
        setAllTaskStatus(subtask, status)
      }
    })

    logger.info(s"Set task $task to status $status")

    task.task.setStatus(status)
  }

}

final case class DuplicateTaskException(jobName: String,
                                        taskName: String,
                                        private val cause: Throwable = None.orNull)
  extends Exception(s"DuplicateTaskException: job $jobName task $taskName", cause)

case class Executable(jobId: Long, task: TaskDesc, dependencies: Seq[TaskDesc], runnable: Runnable)