package com.pyiapa.statcalculator.util

import sys.process._
import java.net.URL
import java.io.File

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
    
    try{
      new URL(url) #> new File(filename) !!
    }catch{
      case noFile: java.io.FileNotFoundException => throw new IllegalArgumentException("Input File missing") 
      case runtime: RuntimeException => throw new IllegalArgumentException("Input File missing")
      case _: Throwable => throw new IllegalArgumentException("Input File missing") 
    }
    
    
  }
  
  
  /**
   * Load CSV file form disk into a DataFrame
   * 
   * @param inputPath the path where the CSV file is located
   * 
   * @return df the DataFrame the contains the loaded file
   * 
   */
  def loadCSVToDF(inputPath: String): DataFrame = {
    
    val spark = MainSparkSession.getCurrentSparkSession()
   
    var df: DataFrame = null
    try{
      df = spark.read.format("csv").option("header", "true").load(inputPath)
    } catch{
     
        case e: AnalysisException => throw new IllegalArgumentException("Input File missing") 
    }
    
    return df
  }
  
}