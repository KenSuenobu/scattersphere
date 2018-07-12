/*
 *    _____            __  __                       __
 *   / ___/_________ _/ /_/ /____  ______________  / /_  ___  ________
 *   \__ \/ ___/ __ `/ __/ __/ _ \/ ___/ ___/ __ \/ __ \/ _ \/ ___/ _ \
 *  ___/ / /__/ /_/ / /_/ /_/  __/ /  (__  ) /_/ / / / /  __/ /  /  __/
 * /____/\___/\__,_/\__/\__/\___/_/  /____/ .___/_/ /_/\___/_/   \___/
 *                                       /_/
 *
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
package io.buildfactory.scattersphere.core.util.spark

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Factory convenience object used to store [[SparkConf]] objects by key name, and generate [[SparkSession]]
  * sessions based on configurations.
  *
  * @since 0.2.0
  */
object SparkCache {

  private val SPARK_CONF_CACHE: ConcurrentMap[String, SparkConf] = new ConcurrentHashMap()

  /** Saves a [[SparkConf]] by key.
    *
    * @param key key to assign
    * @param conf [[SparkConf]] object to store
    */
  def save(key: String, conf: SparkConf): Unit = {
    if (key == null) {
      throw new NullPointerException("Missing key")
    }

    if (conf == null) {
      throw new NullPointerException("Missing SparkConf object.")
    }

    // Appends scattersphere-base to the jars list if not already added.
    var currentJars: Seq[String] = conf.get("spark.jars").split(",")

    if (!currentJars.contains("scattersphere-base")) {
      currentJars = currentJars :+ s"../scattersphere-base/target/scattersphere-base-0.2.3.jar"
    }

    conf.setJars(currentJars)

    SPARK_CONF_CACHE.put(key, conf)
  }

  /** Retrieves the [[SparkConf]] by the specified key.
    *
    * @param key key to retrieve
    * @return [[SparkConf]] or null if not found
    */
  def getConf(key: String): SparkConf = SPARK_CONF_CACHE.get(key)

  /** Creates a new [[SparkSession]] based on the criteria set in the [[SparkConf]] for the given key.
    *
    * @param key key to retrieve
    * @return [[SparkSession]] that is active for that key, or creates a new one if none exists
    */
  def getSession(key: String): SparkSession = SparkSession.builder().config(getConf(key)).getOrCreate()

}
