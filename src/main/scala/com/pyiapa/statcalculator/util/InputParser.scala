package com.pyiapa.statcalculator.util

import sys.process._
import java.net.URL
import java.io.File
import java.io.InputStream
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.commons.io.IOUtils

import com.pyiapa.statcalculator.app.MainSparkSession
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.AnalysisException
import java.lang.IllegalArgumentException
import java.io.FileNotFoundException
import java.lang.RuntimeException
import java.lang.Exception
import java.lang.Throwable

/**
 * Contains various methods to facilitate input parsing
 */
object InputParser {
  
  /**
   * Download a file from a specified URL and save it to disk
   * 
   * @param url the URL to download the file from
   * @param filename the name of the file to be saved on disk
   */
  def downloadFileFromURL(url: String, filename: String) = {
    
    //#> - redirect output
    //!! - executes command and retrieves its output
    try{
      new URL(url) #> new File(filename) !!
    }catch{
      case noFile: java.io.FileNotFoundException => throw new IllegalArgumentException("Input File missing") 
      case runtime: RuntimeException => throw new IllegalArgumentException("Input File missing")
      case _: Throwable => throw new IllegalArgumentException("Input File missing") 
    }
    
    
  }
  
  private def getFilePathFromInputStream(inputPath: String, fileName: String): String = {
    
    val initialStream: InputStream = getClass.getResourceAsStream(inputPath)
    val targetFile = new File(fileName)
    
    try{
      Files.copy(initialStream, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
    }catch{
      case incorrectPath: java.lang.NullPointerException =>  throw new IllegalArgumentException("Incorrect file path")  
    }
    
    IOUtils.closeQuietly(initialStream)
    
    return targetFile.toPath().toString()
  }
  
  /**
   * Load CSV file form disk into a DataFrame
   * 
   * @param inputPath the path where the CSV file is located
   * 
   * @return df the DataFrame the contains the loaded file
   * 
   */
  def loadCSVToDF(inputPath: String, fileName: String): DataFrame = {
    
    val spark = MainSparkSession.getCurrentSparkSession()
    
    var df: DataFrame = null
    try{
      val filePath = getFilePathFromInputStream(inputPath, fileName)
      df = spark.read.format("csv").option("header", "true").load(filePath)
      new File(filePath).deleteOnExit()
    } catch{
     
        case e: AnalysisException => throw new IllegalArgumentException("Input File missing")
        case incorrectPath: IllegalArgumentException =>  throw incorrectPath
    }
    
    return df
  }
  
  
}