package com.scattersphere.core.util.execution

import java.util.concurrent.CompletableFuture

import com.scattersphere.core.util.{JobDesc, RunnableTaskStatus, TaskDesc}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class JobExecutor(engine: ExecutionEngine,
                  job: JobDesc) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def queue(): CompletableFuture[Void] = {
    logger.info(s"Starting for engine $engine job $job")

    logger.info("Creating task execution plan")
    val plan = createExecutionPlan

    logger.info("Setting all tasks to WAITING")
    job.tasks.foreach(task => setAllTaskStatus(task, RunnableTaskStatus.WAITING))

    val topLevelTasks: Seq[TaskDesc] = job.tasks.filter(_.getDependencies.length == 0)

    execute(plan, topLevelTasks, null)
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

  private def execute(plan: Map[String, Executable],
                      topLevelTasks: Seq[TaskDesc],
                      parent: CompletableFuture[Void]): CompletableFuture[Void] = {

    // Please, please, please convert this to Scala.  This makes my eyes bleed.
    var cFuture: CompletableFuture[Void] = CompletableFuture.allOf(
      topLevelTasks.map(task => {
        if (parent != null) {
          return parent.thenRun(task.task)
          // handle exceptions here?
        }

        CompletableFuture.runAsync(task.task)
        //and handle exception
      }): _*
    )

    val dependents = topLevelTasks
      .flatMap(task => task.getDependencies)
      .map(task => plan.get(task.name).get.task)

    if (dependents.length > 0) {
      return execute(plan, dependents, cFuture)
    }

    cFuture
  }
}

final case class DuplicateTaskException(jobName: String,
                                        taskName: String,
                                        private val cause: Throwable = None.orNull)
  extends Exception(s"DuplicateTaskException: job $jobName task $taskName", cause)

case class Executable(jobId: Long, task: TaskDesc, dependencies: Seq[TaskDesc], runnable: Runnable)