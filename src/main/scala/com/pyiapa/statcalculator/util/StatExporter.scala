package com.pyiapa.statcalculator.util

import org.apache.spark.sql.DataFrame
import com.pyiapa.statcalculator.app._
import com.pyiapa.statcalculator.domain._

import java.io.File;

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.InputStream

import java.lang.NullPointerException
import java.lang.RuntimeException

/**
 * Responsible for exporting statistics into an output format to be 
 * used for visualization. Also, responsible for exporting the 
 * necessary visualization scripts
 */
object StatExporter {
  
  final val BASE_OUTPUT_PATH = "output/"
  final val VIS_SCRIPTS_PATH = "/scripts/visualization/"
  final val VIS_SCRIPTS_SUFFIX = ".py"
  final val VIS_SCRIPT_REQ_PATH = "/scripts/"
  final val VIS_SCRIPT_REQ_NAME = "requirements.txt"
  
  /**
   * Export statistic into a single CSV file
   */
  def exportResultToCSV(statistic: Result) = {
    val spark = MainSparkSession.getCurrentSparkSession()
    
    statistic.result.coalesce(1).write.option("header", "true").mode("overwrite").csv(BASE_OUTPUT_PATH + statistic.statName)
  }
  
  /**
   * Export visualization files for this statistic
   */
  @throws[NullPointerException]("if the file doesn't exist")
  def exportVisualizationScripts(statistic: Result) = {
    
    val directory: File = new File(BASE_OUTPUT_PATH + statistic.statName)
    
    if(!directory.exists()){
      directory.mkdir()
    }
    
    if(getClass.getResourceAsStream(VIS_SCRIPTS_PATH + statistic.statName + VIS_SCRIPTS_SUFFIX) != null){
      
     
       //the locaiton of the script to visualize this statistic
       val visulizationScript = getClass.getResourceAsStream(VIS_SCRIPTS_PATH + statistic.statName + VIS_SCRIPTS_SUFFIX)
       
       //the location of the requirements file for the visulization script
       val requirementsFile = getClass.getResourceAsStream(VIS_SCRIPT_REQ_PATH + VIS_SCRIPT_REQ_NAME)
        
       //copy both visualization and requirements script to output directory
       copyVisulaizationScripts(visulizationScript,
                                 BASE_OUTPUT_PATH + statistic.statName + "/"  + statistic.statName + VIS_SCRIPTS_SUFFIX)
       copyVisulaizationScripts(requirementsFile,
                                 BASE_OUTPUT_PATH + statistic.statName + "/"  + VIS_SCRIPT_REQ_NAME)
     
    }
  }
  
  //copy visualization scripts to the output directory
  private[this] def copyVisulaizationScripts(sourceFilename: InputStream, destinationFilename: String) = {
    Files.copy( sourceFilename, Paths.get(destinationFilename), StandardCopyOption.REPLACE_EXISTING)

  }
  
}