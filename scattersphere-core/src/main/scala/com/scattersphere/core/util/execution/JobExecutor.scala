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

import java.util.concurrent.{CompletableFuture, ExecutorService, Executors}
import java.util.function.{Function => JavaFunction}

import com.scattersphere.core.util._

import scala.collection.mutable

/**
  * JobExecutor class
  *
  * This is the heart of the execution engine.  It takes a [[Job]] object, traverses all of the [[Task]]
  * items defined in it, and creates a DAG.  From this DAG, it determines which tasks can be run asynchronously,
  * and which tasks have dependencies.
  *
  * Any tasks that contain dependencies will block until the task they depend on has completed.  Tasks that are
  * "root" tasks (ie. top level tasks) can be run asynchronously as multiple tasks in multiple threads as they
  * see fit.
  *
  * @param job The [[Job]] containing all of the tasks (and dependencies) to run.
  */
class JobExecutor(job: Job) {

  private val taskMap: mutable.HashMap[String, CompletableFuture[Void]] = new mutable.HashMap
  private val executorService: ExecutorService = Executors.newCachedThreadPool

  /**
    * Walks the tree of all tasks for this job, creating an execution DAG.  Since the top-level tasks run using an
    * asynchronous CompletableFuture, it's possible that the tasks will start while the DAG is being generated.
    * This should not affect how the tasks run, however, it may affect synchronization in your top-level application,
    * should you depend on timing or anything of that sort.
    *
    * @return CompletableFuture containing the completed DAG of tasks to execute.
    */
  def queue(): CompletableFuture[Void] = {
    val tasks: Seq[Task] = job.tasks

    generateExecutionPlan(tasks)

    CompletableFuture.allOf(taskMap.values.toSeq: _*)
  }

  private def runTask(task: Task): Unit = {
    task.getStatus match {
      case TaskQueued => {
        task.setStatus(TaskRunning)
        task.task.run()
        task.task.onFinished()
        task.setStatus(TaskFinished)
      }

      case x: TaskStatus => throw new InvalidJobStatusException(task, TaskQueued, x)
    }
  }

  private def walkSubtasks(dependent: Task, tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      taskMap.get(dependent.name) match {
        case Some(_) => println(s"  `- [${dependent.name}: Already queued] Parent=${task.name} has ${task.getDependencies.length} subtasks.")
        case None => {
          val parentFuture: CompletableFuture[Void] = taskMap(task.name)

          if (dependent.async) {
            taskMap.put(dependent.name, parentFuture.thenRunAsync(() => runTask(dependent), executorService))

            println(s"  `- [${dependent.name}: Queued (ASYNC)] Parent=${task.name} has ${task.getDependencies.length} subtasks.")
          } else {
            taskMap.put(dependent.name, parentFuture.thenRun(() => runTask(dependent)))

            println(s"  `- [${dependent.name}: Queued] Parent=${task.name} has ${task.getDependencies.length} subtasks.")
          }
        }
      }

      if (task.getDependencies.nonEmpty) {
        walkSubtasks(task, task.getDependencies)
      }
    })
  }

  private def generateExecutionPlan(tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      if (task.getDependencies.isEmpty) {
        println(s"Task: ${task.name} [ASYNC Root Task]")

        taskMap.put(task.name, CompletableFuture.runAsync(() => runTask(task), executorService))
      } else {
        println(s"Task: ${task.name} task - Walking tree")
        walkSubtasks(task, task.getDependencies)
      }
    })

    println(s"Known task map: ${taskMap.keys}")
  }

}

class InvalidJobStatusException(task: Task,
                                status: TaskStatus,
                                expected: TaskStatus)
  extends Exception(s"InvalidJobStatusException: task ${task.name} set to $status, expected $expected")