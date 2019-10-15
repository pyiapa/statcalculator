package com.pyiapa.statcalculator.service

import com.pyiapa.statcalculator.domain.Result

import org.apache.spark.sql.DataFrame


/**
 * Interface that every statistic must implement.
 * Every statistic must have calculate method that returns a Result
 * object as well as a name for the statistic
 * 
 * @author pyiapa
 * 
 */
trait Statistic {
  
  //the name of the statistic
  var name: String = ""
  
  /**
   * Computes the statistic.
   * @return Result. the result of the computation.
   */
  def calculate(): Result
  
  
  /**
   * Ensures that the input conforms to the correct format.
   * 
   * @param inputDF. the input data frame
   * 
   */
  def checkFormat(inputDF: DataFrame) = {
  }
  
  /**
   * Returns the name of the statistic
   * 
   * @return the name of the statistic
   */
  def getName: String = {
    return name
  }
  
  
}