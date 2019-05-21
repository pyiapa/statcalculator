package com.pyiapa.statcalculator.service

import collection.mutable.HashMap

import com.pyiapa.statcalculator.domain._
import com.pyiapa.statcalculator.util._

import java.lang.NullPointerException


/**
 * Class to manage various statistics
 * 
 * @author pyiapa
 * 
 */
class StatManager {
  
  //collection of statistics
  private[this] var statisticMap = new HashMap[String, Statistic]()
  
  //collection of results from the statistics
  private[this] var resultMap = new HashMap[String, Result]()
  
  /**
   * Registers a statistic with the manager
   * 
   * @param statName the name of the statistic
   * @param stat the statistic to be added
   */
  def registerStatistic(statName: String, stat: Statistic) = {
    statisticMap.put(statName, stat)
  }
  
  /**
   * Calculates the results from all the statistics registered with the manager
   */
  def calculateAllStats = {
      
      if(statisticMap.isEmpty)
        throw new RuntimeException("No statistics are registered")
    
      statisticMap foreach(x => resultMap.put(x._1, x._2.calculate() ) )
  }
  
  /**
   * Displays the results from all the statistics registered with the manager
   */
  def displayAllStats = {
    
    if(!resultMap.isEmpty)
      resultMap foreach (_._2.showResult)
  }
  
  /**
   * Exports all statistics registered with the manager to CSV files
   */
  def exportAllStatsToCSV = {
    
    if(!resultMap.isEmpty)
      resultMap.foreach(x => StatExporter.exportResultToCSV(x._2))  
  }
  
  /**
   * Exports visualization scripts for all statistics registered with the manager
   */
  def exportVisualizationScripts = {
    
    try{
      if(!resultMap.isEmpty)
        resultMap.foreach(x => StatExporter.exportVisualizationScripts(x._2))
    }catch{
      case e: NullPointerException => print("Visualization script missing"); System.exit(1)
    }
  }
  
  /**
   * Calculates the result of the specified statistic
   * 
   * @param statName the name of the statistic
   * 
   */
  def calculateStat(statName: String) = {
    
    val stat = statisticMap.get(statName)
    
    
    if(stat == None)
      throw new RuntimeException("Statistic not found")
    
      resultMap.put(statName, statisticMap(statName).calculate())
  }
  
  /**
   * Returns the result of the supplied statistic
   * 
   * @param statName the name of the statistic
   */
  def getResult(statName: String): Result = {
    
    val statResult = resultMap.getOrElse(statName, null)
    
    if(statResult == null)
      throw new RuntimeException("Statistic not found")
    
    return statResult
  }
  
  /**
   * Checks if a statistic is registered with the manager
   * 
   * @ return whether a statistic is present or not
   */
  def isRegistered(statName: String): Boolean = {
    return statisticMap.contains(statName)
  }
  
  /**
   * Clears the manager from all stats and results
   */
  def clear = {
    statisticMap.clear()
    resultMap.clear() 
  }
 
  
}