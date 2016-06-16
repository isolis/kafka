/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.lang.management.ManagementFactory
import java.util
import java.util.Locale
import javax.management._

import org.apache.log4j.{Level, LogManager, Logger}


object Log4jController {

  private val controller = new Log4jController

  /**
    * Register the given mbean with the platform mbean server,
    * unregistering any mbean that was there before. Note,
    * this method will not throw an exception if the registration
    * fails (since there is nothing you can do and it isn't fatal),
    * instead it just returns false indicating the registration failed.
    * @param mbean The object to register as an mbean
    * @param name The name to register this mbean with
    * @return true if the registration succeeded
    */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer()
      mbs synchronized {
        val objName = new ObjectName(name)
        if(mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception => {
        //error("Failed to register Mbean " + name, e)
        false
      }
    }
  }

  registerMBean(controller, "kafka:type=kafka.Log4jController")

}


/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 * The companion object contains the singleton instance of this class and
 * registers the MBean. The [[kafka.utils.FastLogging]] trait forces initialization
 * of the companion object.
 */
private class Log4jController extends Log4jControllerMBean {

  def getLoggers = {
    val lst = new util.ArrayList[String]()
    lst.add("root=" + existingLogger("root").getLevel.toString)
    val loggers = LogManager.getCurrentLoggers
    while (loggers.hasMoreElements) {
      val logger = loggers.nextElement().asInstanceOf[Logger]
      if (logger != null) {
        val level =  if (logger != null) logger.getLevel else null
        lst.add("%s=%s".format(logger.getName, if (level != null) level.toString else "null"))
      }
    }
    lst
  }


  private def newLogger(loggerName: String) =
    if (loggerName == "root")
      LogManager.getRootLogger
    else LogManager.getLogger(loggerName)


  private def existingLogger(loggerName: String) =
    if (loggerName == "root")
      LogManager.getRootLogger
    else LogManager.exists(loggerName)


  def getLogLevel(loggerName: String) = {
    val log = existingLogger(loggerName)
    if (log != null) {
      val level = log.getLevel
      if (level != null)
        log.getLevel.toString
      else "Null log level."
    }
    else "No such logger."
  }


  def setLogLevel(loggerName: String, level: String) = {
    val log = newLogger(loggerName)
    if (!loggerName.trim.isEmpty && !level.trim.isEmpty && log != null) {
      log.setLevel(Level.toLevel(level.toUpperCase(Locale.ROOT)))
      true
    }
    else false
  }

}


private trait Log4jControllerMBean {
  def getLoggers: java.util.List[String]
  def getLogLevel(logger: String): String
  def setLogLevel(logger: String, level: String): Boolean
}

