package com.pyiapa.statcalculator.app

import java.lang.IllegalArgumentException
import java.lang.Exception

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import com.pyiapa.statcalculator.util._
import com.pyiapa.statcalculator.service._


/**
 * Calculates summary statistics for various expenses
 * 
 * @author pyiapa
 * 
 */
object Main {
  
  //input data
  final val URL = "https://2019-tech-lisa.s3.eu-west-3.amazonaws.com/lisa_expenses_data.csv"
  
  //the file that we'll save the downloaded data
  final val FILE_NAME = "lisa_expenses_data.csv"
  
  /**
   * Main driver for the statcalculator application
   */
  def main(args: Array[String]) {
    
    //entry point to spark programming with DataFrame and DataSet APIs
    val spark = MainSparkSession.getCurrentSparkSession()
    
    //download the dataset
    try{
      InputParser.downloadFileFromURL(URL, FILE_NAME)
    }catch{
      case e: IllegalArgumentException => print(e.getMessage); System.exit(1)
    }
    
    //dataframe to hold the input data
    var lisaExpensesDF: DataFrame = null 
    
    //load the dataset
    try{
      lisaExpensesDF = InputParser.loadCSVToDF(FILE_NAME)
    }catch{
      case e: IllegalArgumentException => print(e.getMessage); System.exit(1)
    }
    
    
    //create an object to manage all the statistics we need
    val statisticsManager =  new StatManager()
    
    //register statistic to calculate the most visited country
    val mostVisitedCountry = new MostVisitedCountry(lisaExpensesDF)
    statisticsManager.registerStatistic(mostVisitedCountry.name, mostVisitedCountry)
    
    //register statistic to calculate the most visited country in 2018
    val mostVisitedCountry2018 = new MostVisitedCountry2018(lisaExpensesDF)
    statisticsManager.registerStatistic(mostVisitedCountry2018.name, mostVisitedCountry2018)
    
    //register statistic to calculate the costliest month
    val costliestMonth = new CostliestMonth(lisaExpensesDF)
    statisticsManager.registerStatistic(costliestMonth.name, costliestMonth)
    
    //register statistic to calculate the daily spending for the last 60 days
    val dailySpending = new DailySpending(lisaExpensesDF)
    statisticsManager.registerStatistic(dailySpending.name, dailySpending)
    
    //calculate statistics and export results into csv files for visualization
    statisticsManager.calculateAllStats
    statisticsManager.exportAllStatsToCSV
    statisticsManager.exportVisualizationScripts
  }  
}