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
package com.scattersphere.core.util.execution

import java.util.concurrent.CompletableFuture

import com.scattersphere.core.util.{JobDesc, RunnableTaskStatus, TaskDesc}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * JobExecutor class
  *
  * This is the heart of the execution engine.  It takes a [[JobDesc]] object, traverses all of the [[TaskDesc]]
  * items defined in it, and creates a DAG.  From this DAG, it determines which tasks can be run asynchronously,
  * and which tasks have dependencies.
  *
  * Any tasks that contain dependencies will block until the task they depend on has completed.  Tasks that are
  * "root" tasks (ie. top level tasks) can be run asynchronously as multiple tasks in multiple threads as they
  * see fit.  The only restriction is based on the Java [[java.util.concurrent.Executor]] object implementation
  * that they choose to use.
  *
  * @param job The [[JobDesc]] containing all of the tasks (and dependencies) to run.
  */
class JobExecutor(job: JobDesc) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * This queues a series of tasks to run.  It takes the job description passed into the constructor,
    * builds an internal execution plan (a map of tasks by name with an [[Executable]] object), changes each
    * job status to [[RunnableTaskStatus.WAITING]], and executes the plan.
    *
    * @return [[CompletableFuture]] containing the execution DAG.
    */
  def queue(): CompletableFuture[Void] = {
    logger.info("Creating task execution plan")
    val plan = createExecutionPlan

    logger.info("Setting all tasks to WAITING")
    job.tasks.foreach(task => setAllTaskStatus(task, RunnableTaskStatus.WAITING))

    val topLevelTasks: Seq[TaskDesc] = job.tasks.filter(_.getDependencies.isEmpty)

    execute(plan, topLevelTasks, null)
  }

  /**
    * Creates an execution plan by mapping each task with a wrapped [[Executable]].  This also checks
    * to see if a task has already been defined multiple times.  If so, the execution plan will fail,
    * as duplicate tasks cannot be added to a job.
    *
    * Job IDs are currently assigned the current time in milliseconds, but this will be done using an
    * atomic integer eventually.  This will likely change once the distributed model comes into play.
    *
    * @return [[Map]] containing the String -> Executable list.
    */
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

  /**
    * Traverses every possible task and changes its status to a specified value.
    *
    * @param task The top level [[TaskDesc]] object to traverse.
    * @param status The status to set each task.
    */
  private def setAllTaskStatus(task: TaskDesc, status: RunnableTaskStatus.Value): Unit = {
    task.getDependencies.foreach(subtask => {
      logger.info(s"Set subtask $subtask to status $status")

      subtask.task.setStatus(status)

      if (subtask.getDependencies.nonEmpty) {
        setAllTaskStatus(subtask, status)
      }
    })

    logger.info(s"Set task $task to status $status")

    task.task.setStatus(status)
  }

  /**
    * This creates the [[CompletableFuture]] DAG.  It walks each top level task, then walks each
    * dependency underneath each top level task, assigning an execution order based on its dependency.  This
    * function is highly recursive, as it will walk each top level task, then cycle through each tree of
    * dependencies, and apply the parent's [[CompletableFuture]] with a thenRun call.  This will add
    * the functionality of a task running after another task completes in a synchronous manner.
    *
    * Therefore, the top level tasks all run asynchronously, and their tasks will run asynchronously based on the
    * execution order of the parent.
    *
    * @param plan The entire plan of wrapped tasks as a map.
    * @param topLevelTasks The top-level tasks that can be run asynchronously.
    * @param parent The current parent [[CompletableFuture]] to add to (thenRun) if traversing a dependent task.
    * @return A - hopefully complete - [[CompletableFuture]] DAG to execute.
    */
  private def execute(plan: Map[String, Executable],
                      topLevelTasks: Seq[TaskDesc],
                      parent: CompletableFuture[Void]): CompletableFuture[Void] = {

    // Please, please, please convert this to Scala.  This makes my eyes bleed.
    var cFuture: CompletableFuture[Void] = CompletableFuture.allOf(
      topLevelTasks.map(task => {
        if (parent != null) {
          logger.info(s"Appending $task to parent task $parent")
          return parent.thenRun(new Runnable {
            override def run(): Unit = {
              if (task.task.getStatus() == RunnableTaskStatus.CANCELED) {
                logger.info(s"Skipping execution of task ${task.name}; job is canceled.")
                return
              }

              logger.info(s"Setting task ${task.name} status to RUNNING.")
              task.task.setStatus(RunnableTaskStatus.RUNNING)
              task.task.run()

              logger.info(s"Setting task ${task.name} status to COMPLETED.")
              task.task.setStatus(RunnableTaskStatus.COMPLETED)
            }
          })
          // handle exceptions here?
        }

        logger.info(s"Preparing: Task $task will run asynchronously as a root level task")
        CompletableFuture.runAsync(new Runnable {
          override def run(): Unit = {
            if (task.task.getStatus() == RunnableTaskStatus.CANCELED) {
              logger.info(s"[Async] Skipping execution of task ${task.name}; job is canceled.")
              return
            }

            logger.info(s"[Async] Setting task ${task.name} status to RUNNING.")
            task.task.setStatus(RunnableTaskStatus.RUNNING)
            task.task.run()

            logger.info(s"[Async] Setting task ${task.name} status to COMPLETED.")
            task.task.setStatus(RunnableTaskStatus.COMPLETED)
          }
        })
        //and handle exception
      }): _*
    )

    val dependents = topLevelTasks
      .flatMap(task => task.getDependencies)
      .map(task => plan(task.name).task)

    if (dependents.nonEmpty) {
      logger.info(s"This task has ${dependents.length} dependent(s)!  Traversing dependents to append tasks.")
      return execute(plan, dependents, cFuture)
    }

    logger.info(s"Completed DAG generation.  Future: $cFuture")
    cFuture
  }
}

final case class DuplicateTaskException(jobName: String,
                                        taskName: String,
                                        private val cause: Throwable = None.orNull)
  extends Exception(s"DuplicateTaskException: job $jobName task $taskName", cause)

/**
  * Temporary case class to store a task, its dependents, and [[Runnable]] in which to run the task.
  *
  * @param jobId A Job ID.
  * @param task The [[TaskDesc]] to run.
  * @param dependencies The [[TaskDesc]]'s dependency tree.
  * @param runnable The [[Runnable]] that can be run in a [[Thread]]
  */
case class Executable(jobId: Long, task: TaskDesc, dependencies: Seq[TaskDesc], runnable: Runnable)
