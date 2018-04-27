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

  private val completableFutureDag: mutable.HashMap[String, CompletableFuture[Void]] = new mutable.HashMap()
  private val topLevelTasks: mutable.HashMap[String, CompletableFuture[Void]] = new mutable.HashMap()

  /**
    * This queues a series of tasks to run.  It takes the job description passed into the constructor,
    * builds an internal execution plan (a map of tasks by name with an [[Executable]] object), changes each
    * job status to [[RunnableTaskStatus.WAITING]], and executes the plan.
    *
    * @return [[CompletableFuture]] containing the execution DAG.
    */
  def queue(): CompletableFuture[Void] = {
    val plan = createExecutionPlan

    println("Setting all tasks to WAITING")
    job.tasks.foreach(task => setAllTaskStatus(task, RunnableTaskStatus.WAITING))

//    val topLevelTasks: Seq[TaskDesc] = job.tasks.filter(_.getDependencies.isEmpty)

    execute(plan)
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
    println("Creating execution plan.")

    val plan: mutable.HashMap[String, Executable] = new mutable.HashMap()

    job.tasks.foreach(task => {
      if (plan.getOrElse(task.name, null) != null) {
        throw new DuplicateTaskException(job.name, task.name)
      }

      // Filter out any tasks that contain a dependency with this task's name.
      val dependentTasks = job.tasks.filter(_.getDependencies.contains(task))

      println(s"Adding: task=$task dependentTasks=$dependentTasks")

      plan.put(task.name, new Executable(System.currentTimeMillis(), task, dependentTasks))

      if (task.getDependencies.isEmpty) {
        println(s"Creating asynchronous task for ${task.name}")
        topLevelTasks.put(task.name, CompletableFuture.runAsync(task.task))
      }
    })

    println(s"Created plan: ${plan.toMap}")
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
      println(s"Set subtask $subtask to status $status")

      subtask.task.setStatus(status)

      if (subtask.getDependencies.nonEmpty) {
        setAllTaskStatus(subtask, status)
      }
    })

    println(s"Set task $task to status $status")

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
    * @return A - hopefully complete - [[CompletableFuture]] DAG to execute.
    */
  private def execute(plan: Map[String, Executable]): CompletableFuture[Void] = {

    // Please, please, please convert this to Scala.  This makes my eyes bleed.
//    var cFuture: CompletableFuture[Void] = CompletableFuture.allOf(
//      topLevelTasks.map(task => {
//        println(s"Preparing: Task $task will run asynchronously as a root level task")
//        CompletableFuture.runAsync(task.task)
//      }): _*
//    )

    // Set dependencies here
    topLevelTasks.keys.foreach(taskKey => {
      val cFuture: CompletableFuture[Void] = topLevelTasks(taskKey)

      plan(taskKey).dependencies.foreach(subtask => {
        val runnable: Runnable = plan(subtask.name).task.task

        cFuture.thenRun(runnable)
        topLevelTasks.put(taskKey, cFuture)
      })
    })

    val cFuture: CompletableFuture[Void] = CompletableFuture.allOf(topLevelTasks.values.toList: _*)

    println(s"Completed DAG generation.  Future: $cFuture")
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
  */
case class Executable(jobId: Long, task: TaskDesc, dependencies: Seq[TaskDesc])
