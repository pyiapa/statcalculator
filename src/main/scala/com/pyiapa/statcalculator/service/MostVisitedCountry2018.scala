package com.pyiapa.statcalculator.service

import com.pyiapa.statcalculator.domain.Result

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
 
/**
 * Finds the most visited country in 2018
 * 
 * @author pyiapa
 * 
 */
class MostVisitedCountry2018(val inputDF: DataFrame) extends ExpensesStatistic{
  
   name = "most_visited_country_2018"
   
   /**
    * Computes the  most visited country in 2018
    * @return Result the result of the calculation
    */
   override def calculate(): Result = {
     
     //ensure correct input format
     try{
       checkFormat(inputDF)
     }catch{
       case e: RuntimeException => e.getMessage(); System.exit(1)
     }
     
     //retain only entries where lisa visited in the year 2018,
     //count the occurrence of each country, put it in a new column called count,
     //sort them in descending order and get the top result which is essentially
     //the country with the highest number of occurrences (i.e. most visited) in 2018
     val visitedCountryy2018DF = inputDF.filter("date LIKE '%2018'")
                                        .groupBy("location")
                                        .agg(count("location").alias("Count"))
                                        .sort(desc("Count"))
                                        .limit(1)
      
     
     return new Result(name, visitedCountryy2018DF)
   }
      
}