package com.pyiapa.statcalculator.service

import com.pyiapa.statcalculator.domain.Result


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
   * Returns the name of the statistic
   * 
   * @return the name of the statistic
   */
  def getName: String = {
    return name
  }
  
  
}