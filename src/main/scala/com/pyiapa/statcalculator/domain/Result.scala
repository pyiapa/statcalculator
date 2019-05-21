package com.pyiapa.statcalculator.domain


import org.apache.spark.sql.DataFrame
import com.pyiapa.statcalculator.app.MainSparkSession

import java.io.File;

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.InputStream


/**
 * Class to hold the result from calculating a statistic
 * 
 * @author pyiapa
 */
class Result (var statName: String, var result: DataFrame) {
  
  
  /**
   * Print result into the standard output
   */
  def showResult = {
    result.show(result.count().toInt, false)
  }
  
  
}