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

package io.buildfactory.scattersphere.core.util.logging

import org.slf4j.{Logger, LoggerFactory}

/** Trait package that adds a logger facility to the underlying class as a convenience.
  * Once this class is extended to your class, a "logger" object is available for which to log messages
  * against.
  *
  * @since 0.2.1
  */
trait SimpleLogger {

  @transient
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

}
