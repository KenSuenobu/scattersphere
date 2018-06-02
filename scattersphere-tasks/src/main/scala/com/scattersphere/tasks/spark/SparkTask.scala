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

package com.scattersphere.tasks.spark

import com.scattersphere.core.util.RunnableTask
import com.scattersphere.core.util.spark.SparkCache
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SparkSession

/** Spark-based task that provides a [[SparkSession]] and [[SparkContext]] object for use by the run() function.
  *
  * @param sparkConfKey key which stores the [[SparkCache]] information for the [[SparkConf]] object.
  * @since 0.2.0
  */
class SparkTask(sparkConfKey: String) extends RunnableTask with LazyLogging {

  private val spark: SparkSession = SparkCache.getSession(sparkConfKey)

  /** Retrieves the current [[SparkSession]]
    *
    * @return [[SparkSession]] retrieved by sparkConfKey
    */
  def getSession(): SparkSession = spark

  /** Retrieves the [[SparkContext]] for the current [[SparkSession]]
    *
    * @return [[SparkContext]] for the current [[SparkSession]]
    */
  def getContext(): SparkContext = spark.sparkContext

  override def run(): Unit = throw new NotImplementedError("Need to override run.")

}
