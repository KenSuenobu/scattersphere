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
package io.buildfactory.scattersphere.core.server

class Main {

  def header(): Unit = {
    System.err.println("    _____            __  __                       __")
    System.err.println("   / ___/_________ _/ /_/ /____  ______________  / /_  ___  ________")
    System.err.println("   \\__ \\/ ___/ __ `/ __/ __/ _ \\/ ___/ ___/ __ \\/ __ \\/ _ \\/ ___/ _ \\")
    System.err.println("  ___/ / /__/ /_/ / /_/ /_/  __/ /  (__  ) /_/ / / / /  __/ /  /  __/")
    System.err.println(" /____/\\___/\\__,_/\\__/\\__/\\___/_/  /____/ .___/_/ /_/\\___/_/   \\___/")
    System.err.println("                                       /_/")
  }

  def showHelp(): Unit = {
    System.err.println("\nUsage:")
  }

}

object Main extends App {

  val main: Main = new Main()

  main.header()

  if (args.length == 1) {
    main.showHelp()
    System.exit(-1)
  }

}