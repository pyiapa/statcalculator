package com.pyiapa.statcalculator.service

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

import com.pyiapa.statcalculator.service._
import com.pyiapa.statcalculator.domain._
import com.pyiapa.statcalculator.app._
import com.pyiapa.statcalculator.util._

import java.io.File;

/**
 * Includes various tests for the StatManager class
 * 
 * @author pyiapa
 * 
 */
class StatManagerTest extends FunSuite {
    
    val statManager = new StatManager()
    
    val fakeStatistic = new Statistic {
      name = "fake_statistic"
      override def calculate(): Result = {
        
        val spark = MainSparkSession.getCurrentSparkSession()
        import spark.implicits._
        
        val fakeInput = Seq( ("test value", 1) ).toDF("Title", "Count")
        
        return new Result(name, fakeInput)
      }
    }
  
    test("Test registerStatistic method for correct result") {
     
      statManager.registerStatistic(fakeStatistic.name, fakeStatistic)

      assert(statManager.isRegistered(fakeStatistic.name) == true)
    }
    
    test("Trying to calculate a non-existing statistic should throw RuntimeException") {
    
      val thrown = intercept[RuntimeException] {
       statManager.calculateStat("Incorrect Name")
      }
      assert(thrown.getMessage === "Statistic not found")
    }
    
    test("Test calculateStatistic method for correct result") {
     
      statManager.registerStatistic(fakeStatistic.name, fakeStatistic)
      
      statManager.calculateStat(fakeStatistic.name)
      
      val result = statManager.getResult(fakeStatistic.name).result.select("Count").first().get(0)
      
      assert(result == 1)
    }
    
    test("Trying to get a non-existing resut should throw RuntimeException") {
    
      val thrown = intercept[RuntimeException] {
       statManager.getResult("Incorrect Name")
      }
      assert(thrown.getMessage === "Statistic not found")
    }
    
    test("Trying to calculate all stats while on stats exist should throw RuntimeException") {
      
      val thrown = intercept[RuntimeException] {
       statManager.clear
       statManager.calculateAllStats
      }
      assert(thrown.getMessage === "No statistics are registered")
    }
    
    test("Test exportAllStatsToCSV method for exporting csv") {
     
      statManager.registerStatistic(fakeStatistic.name, fakeStatistic)
      
      statManager.calculateStat(fakeStatistic.name)
      
      statManager.exportAllStatsToCSV
      
      val dir: File = new File("output/fake_statistic")
      val requiredFileExtensions = List("csv")
      
      val csv_files =  getListOfFiles(dir, requiredFileExtensions)
    
      assert(!csv_files.isEmpty)
      
      deleteRecursively(new File("output"))
    }
    
    test("Test exportAllStatsToCSV method for correct output") {
     
      statManager.registerStatistic(fakeStatistic.name, fakeStatistic)
      
      statManager.calculateStat(fakeStatistic.name)
      
      statManager.exportAllStatsToCSV
      
      val dir: File = new File("output/fake_statistic")
      val requiredFileExtensions = List("csv")
      
      val csv_files =  getListOfFiles(dir, requiredFileExtensions)
      
      val df = InputParser.loadCSVToDF(csv_files(0).toPath().toString())
      
      val result = df.select("Title").first().get(0)
      
      assert(result === "test value")
      
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