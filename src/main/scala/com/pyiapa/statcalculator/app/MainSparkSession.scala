package com.pyiapa.statcalculator.app

import org.apache.log4j._
import org.apache.spark.sql.SparkSession


/**
 * Singleton class to hold an instance of Spark Session
 * 
 * @author pyiapa
 * 
 */
object MainSparkSession {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark: SparkSession = SparkSession.builder().appName("Main").master("local[*]").getOrCreate
  
  /**
   * Returns the current SparkSession
   */
  def getCurrentSparkSession() = {spark}
 
}