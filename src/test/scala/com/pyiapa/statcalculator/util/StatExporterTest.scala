package com.pyiapa.statcalculator.util

import org.scalatest.FunSuite

import com.pyiapa.statcalculator.app._
import com.pyiapa.statcalculator.util._
import com.pyiapa.statcalculator.domain._

import java.io.File;

import java.lang.RuntimeException
import java.lang.NullPointerException

/**
 * Contains various tests for the StatExpolorer utility
 */
class StatExporterTest extends FunSuite{
  
  final val FAKE_VIS_SCRIPT = "../statcalculator/src/test/resources/input/fake_statistic.csv"
  
  val spark = MainSparkSession.getCurrentSparkSession()
  import spark.implicits._
  
  val fakeInput = Seq( ("test value", 1) ).toDF("Title", "Count")
  val statName = "empty_statistic"
  
  val fakeResult = new Result(statName, fakeInput)
      
  test("Test exportAllStatsToCSV method") {
     
     StatExporter.exportResultToCSV(fakeResult) 
      
      val dir: File = new File("output/empty_statistic")
      val requiredFileExtensions = List("csv")
      
      val csv_files =  getListOfFiles(dir, requiredFileExtensions)
    
      assert(!csv_files.isEmpty)
      
      deleteRecursively(dir)
   }
  
   test("Test exportVisualizationScripts method") {
     
     StatExporter.exportVisualizationScripts(fakeResult) 
      
      val dir: File = new File("output/empty_statistic")
      val requiredFileExtensions = List("py")
      
      val py_files =  getListOfFiles(dir, requiredFileExtensions)
    
      assert(!py_files.isEmpty)
      
      deleteRecursively(new File("output"))
   }
   
        
   //get files from a directory with specified extensions
   def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
     dir.listFiles.filter(_.isFile).toList.filter { file =>
       extensions.exists(file.getName.endsWith(_))
     }
   }
    
    //delete recursively a directory
   def deleteRecursively(file: File): Unit = {
     if (file.isDirectory)
       file.listFiles.foreach(deleteRecursively)
     if (file.exists && !file.delete)
       throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
   }       
 
  
}