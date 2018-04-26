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

package com.scattersphere.core.util

import com.scattersphere.core.util.RunnableTaskStatus.QUEUED

/**
  * RunnableTask trait
  *
  * This is a trait that describes how to run a job.  It implements a few helper functions that are applied to all
  * [[RunnableTask]]s.  New tasks are automatically set with a status of QUEUED.
  */
trait RunnableTask extends Runnable {

  private var initVars: Map[String, String] = Map()
  private var taskStatus: RunnableTaskStatus.Value = QUEUED

  def init(vars: Map[String, String]): Unit = {
    initVars = vars
  }

  def getSettings(): Map[String, String] = initVars

  def setStatus(status: RunnableTaskStatus.Value): Unit = taskStatus = status

  def getStatus(): RunnableTaskStatus.Value = taskStatus

}

/**
  * RunnableTaskStatus Enum
  *
  * This describes the status of a currently running job.
  */

object RunnableTaskStatus extends Enumeration {

  val RunnableTaskStatus = Value
  val QUEUED, WAITING, RUNNING, COMPLETED, CANCELED = Value

}
