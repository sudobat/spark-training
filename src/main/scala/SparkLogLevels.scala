package es.novaquality.spark

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.internal.Logging

object SparkLogLevels extends Logging {

  def setLogLevelToWarn(): Unit = {
    logInfo("Setting log level to [WARN]")
    Configurator.setRootLevel(Level.WARN)
  }
}
