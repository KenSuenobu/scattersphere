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

/** Storage class containing start, end, and elapsed times for measuring performance.
  *
  * @since 0.2.1
  */
class Statistics {

  private var startTime: Long = 0
  private var endTime: Long = 0

  /** Triggers the start of the statistics gathering.  If already trigerred, the new time is ignored. */
  def triggerStart(): Unit = if (startTime == 0) startTime = System.currentTimeMillis()

  /** Triggers the end of the statistics gathering.  If already triggered, the new time is ignored. */
  def triggerEnd(): Unit = if (endTime == 0) endTime = System.currentTimeMillis()

  /** Retrieves the start time in milliseconds.
    *
    * @return Long containing the start time
    */
  def getStartTime: Long = startTime

  /** Retrieves the end time in milliseconds.
    *
    * @return Long containing the end time
    */
  def getEndTime: Long = endTime

  /** Retrieves the total elapsed time in milliseconds.
    *
    * @return Long containing end - start time
    */
  def getElapsedTime: Long = getEndTime - getStartTime

  override def toString: String = s"Statistics{startTime=$startTime, endTime=$endTime}"

}
