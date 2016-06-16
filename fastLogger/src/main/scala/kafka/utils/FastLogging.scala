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

import org.apache.log4j.Logger

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

object MacroLogger {
  def info(c: Context)(msg: c.Expr[String]) = {
    import c.universe._
    q"if (logger.isInfoEnabled()) logger.info(msgWithLogIdent($msg))"
  }
}


trait FastLogging {
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  protected var logIdent: String = null

  /**
    * Do the given action and log any exceptions thrown without rethrowing them
    * @param log The log method to use for logging. E.g. logger.warn
    * @param action The action to execute
    */
  def swallow(log: (Object, Throwable) => Unit, action: => Unit) {
    try {
      action
    } catch {
      case e: Throwable => log(e.getMessage(), e)
    }
  }



  // Force initialization to register Log4jControllerMBean
  private val log4jController = Log4jController

  protected def msgWithLogIdent(msg: String) =
    if(logIdent == null) msg else logIdent + msg

  def trace(msg: => String): Unit = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg))
  }
  def trace(e: => Throwable): Any = {
    if (logger.isTraceEnabled())
      logger.trace(logIdent,e)
  }
  def trace(msg: => String, e: => Throwable) = {
    if (logger.isTraceEnabled())
      logger.trace(msgWithLogIdent(msg),e)
  }
  def swallowTrace(action: => Unit) {
    swallow(logger.trace, action)
  }

  def debug(msg: => String): Unit = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg))
  }
  def debug(e: => Throwable): Any = {
    if (logger.isDebugEnabled())
      logger.debug(logIdent,e)
  }
  def debug(msg: => String, e: => Throwable) = {
    if (logger.isDebugEnabled())
      logger.debug(msgWithLogIdent(msg),e)
  }
  def swallowDebug(action: => Unit) {
    swallow(logger.debug, action)
  }

  def info(msg: String): Unit = macro MacroLogger.info

  def infoOld(msg: => String): Unit = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg))
  }

  def info(e: => Throwable): Any = {
    if (logger.isInfoEnabled())
      logger.info(logIdent,e)
  }
  def info(msg: => String,e: => Throwable) = {
    if (logger.isInfoEnabled())
      logger.info(msgWithLogIdent(msg),e)
  }
  def swallowInfo(action: => Unit) {
    swallow(logger.info, action)
  }

  def warn(msg: => String): Unit = {
    logger.warn(msgWithLogIdent(msg))
  }
  def warn(e: => Throwable): Any = {
    logger.warn(logIdent,e)
  }
  def warn(msg: => String, e: => Throwable) = {
    logger.warn(msgWithLogIdent(msg),e)
  }
  def swallowWarn(action: => Unit) {
    swallow(logger.warn, action)
  }
  def swallow(action: => Unit) = swallowWarn(action)

  def error(msg: => String): Unit = {
    logger.error(msgWithLogIdent(msg))
  }
  def error(e: => Throwable): Any = {
    logger.error(logIdent,e)
  }
  def error(msg: => String, e: => Throwable) = {
    logger.error(msgWithLogIdent(msg),e)
  }
  def swallowError(action: => Unit) {
    swallow(logger.error, action)
  }

  def fatal(msg: => String): Unit = {
    logger.fatal(msgWithLogIdent(msg))
  }
  def fatal(e: => Throwable): Any = {
    logger.fatal(logIdent,e)
  }
  def fatal(msg: => String, e: => Throwable) = {
    logger.fatal(msgWithLogIdent(msg),e)
  }
}
