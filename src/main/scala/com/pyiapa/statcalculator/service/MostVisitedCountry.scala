package com.pyiapa.statcalculator.service


import com.pyiapa.statcalculator.domain.Result

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
 
/**
 * Finds the most visited country ever
 * 
 * @author pyiapa
 * 
 */
class MostVisitedCountry(val inputDF: DataFrame) extends Statistic with ExpensesFormatChecker{
  
   name = "most_visited_country"
   
   /**
    * Computes the  most visited country ever
    * @return Result the result of the calculation
    */
   override def calculate(): Result = {
     
     //ensure correct input format
     try{
       checkFormat(inputDF)
     }catch{
       case e: RuntimeException => e.getMessage(); System.exit(1)
     }
     
     //count the occurrence of each country, put it in a new column called count,
     //sort them in descending order and get the top result which is essentially
     //the country with the highest number of occurrences (i.e. most visited)
     val mostVisitedCOuntryDF = inputDF.groupBy("location")
                                       .agg(count("location").alias("Count"))    
                                       .sort(desc("Count"))
                                       .limit(1)
      
     
     return new Result(name, mostVisitedCOuntryDF)
   }
      
}