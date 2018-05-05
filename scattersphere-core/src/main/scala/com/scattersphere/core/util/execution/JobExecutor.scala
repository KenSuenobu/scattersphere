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

import java.util.concurrent.{CompletionException, _}
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
  private val lockObject: Object = new Object

  private var completableFuture: CompletableFuture[Void] = null

  /**
    * Walks the tree of all tasks for this job, creating an execution DAG.  Since the top-level tasks run using an
    * asynchronous CompletableFuture, it's possible that the tasks will start while the DAG is being generated.
    * This should not affect how the tasks run, however, it may affect synchronization in your top-level application,
    * should you depend on timing or anything of that sort.
    *
    * @return CompletableFuture containing the completed DAG of tasks to execute.
    */
  def queue(): JobExecutor = {
    val tasks: Seq[Task] = job.tasks

    generateExecutionPlan(tasks)

    completableFuture = CompletableFuture
      .allOf(taskMap.values.toSeq: _*)
      .whenComplete((_, _) => {
        job.getStatus() match {
          case JobRunning => job.setStatus(JobFinished)
          case _ => // Do nothing; keep state stored
        }
      })

    this
  }

  /**
    * Triggers the JobExecutor to run the job immediately, returning after the lock for the job has been released.
    * Use of this behavior will cause the job to run in the background, so logging and other verbose functions could
    * interfere with output from other functions in your code.
    */
  def runNonblocking(): Unit = {
    job.setStatus(JobRunning)
    unlock
  }

  /**
    * Triggers the JobExecutor to run the job, but blocks all further execution until the job has completed.  This
    * function will return after the job completes.
    */
  def runBlocking(): Unit = {
    job.setStatus(JobRunning)
    unlock

    completableFuture.join
  }

  private def unlock(): Unit = {
    lockObject.synchronized {
      lockObject.notifyAll
    }
  }

  private def runTask(task: Task): Unit = {
    task.getStatus match {
      case TaskQueued => {
        task.setStatus(TaskRunning)
        task.task.run()
        task.task.onFinished()
        task.setStatus(TaskFinished)
      }

      case _ => {
        val failedTaskException = new InvalidTaskStateException(task, task.getStatus, TaskQueued)

        task.setStatus(TaskFailed(failedTaskException))
        job.setStatus(JobFailed(failedTaskException))
      }
    }
  }

  private def runTaskWithLock(task: Task): Runnable = () => {
    lockObject.synchronized {
      lockObject.wait()
    }

    runTask(task)
  }

  private def toJavaFunction[A, B](f: Function1[A, B]) = new JavaFunction[A, B] {
    override def apply(a: A): B = f(a)
  }

  private def walkSubtasks(dependent: Task, tasks: Seq[Task]): Unit = {
    tasks.foreach(task => {
      taskMap.get(dependent.name) match {
        case Some(_) => println(s"  `- [${dependent.name}: Already queued] Parent=${task.name} has ${task.getDependencies.length} subtasks.")
        case None => {
          val parentFuture: CompletableFuture[Void] = taskMap(task.name)

          if (dependent.async) {
            val cFuture: CompletableFuture[Void] = parentFuture.thenRunAsync(() => runTask(dependent), executorService)

            taskMap.put(dependent.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) => {
              f match {
                case ex: CompletionException => {
                  dependent.task.onException(ex.getCause)
                  dependent.setStatus(TaskFailed(ex.getCause))
                  job.setStatus(JobFailed(ex.getCause))
                }
                case _ => {
                  dependent.task.onException(f)
                  dependent.setStatus(TaskFailed(f))
                  job.setStatus(JobFailed(f))
                }
              }

              throw f
            })))

            println(s"  `- [${dependent.name}: Queued (ASYNC)] Parent=${task.name} has ${task.getDependencies.length} subtasks.")
          } else {
            val cFuture: CompletableFuture[Void] = parentFuture.thenRun(() => runTask(dependent))

            taskMap.put(dependent.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) => {
              f match {
                case ex: CompletionException => {
                  dependent.task.onException(ex.getCause)
                  dependent.setStatus(TaskFailed(ex.getCause))
                  job.setStatus(JobFailed(ex.getCause))
                }
                case _ => {
                  dependent.task.onException(f)
                  dependent.setStatus(TaskFailed(f))
                  job.setStatus(JobFailed(f))
                }
              }

              throw f
            })))

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

        val cFuture: CompletableFuture[Void] = CompletableFuture.runAsync(runTaskWithLock(task), executorService)

        taskMap.put(task.name, cFuture.exceptionally(toJavaFunction[Throwable, Void]((f: Throwable) => {
          f match {
            case ex: CompletionException => {
              task.task.onException(ex.getCause)
              task.setStatus(TaskFailed(ex.getCause))
              job.setStatus(JobFailed(ex.getCause))
            }
            case _ => {
              task.task.onException(f)
              task.setStatus(TaskFailed(f))
              job.setStatus(JobFailed(f))
            }
          }

          throw f
        })))
      } else {
        println(s"Task: ${task.name} task - Walking tree")
        walkSubtasks(task, task.getDependencies)
      }
    })

    println(s"Known task map: ${taskMap.keys}")
  }

}

class InvalidTaskStateException(task: Task,
                                status: TaskStatus,
                                expected: TaskStatus)
  extends Exception(s"InvalidTaskStateException: task ${task.name} set to $status, expected $expected")