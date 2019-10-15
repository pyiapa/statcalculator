package com.pyiapa.statcalculator.service

import com.pyiapa.statcalculator.domain.Result

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
 
/**
 * Calculates the daily spending for the last 60 days
 * 
 * Note that there is an assumption here that 
 * a reimbursed month accounts to zero cost.
 * 
 * @author pyiapa
 * 
 */
class DailySpending(val inputDF: DataFrame) extends ExpensesStatistic{
  
   name = "daily_spending_last_60_days"
   
   /**
    * Calculates the daily spending for the last 60 days
    * 
    * Note that there is an assumption here that 
    * a reimbursed month accounts to zero cost.
    * 
    * @return Result the result of the calculation
    */
   override def calculate(): Result = {
    
    //ensure correct input format
    try{
      checkFormat(inputDF)
    }catch{
      case e: RuntimeException => e.getMessage(); System.exit(1)
    } 
     
    //adjust the cost to account for reimbursed items. Here I'm assuming that if an expense is
    //reimbursed then the cost for that item will be 0 for Lisa
    val adjustedCostDF = inputDF.withColumn("amount", when(col("reimbursed").equalTo("true"), 0).otherwise(col("amount") ) )
    
    //aggregate the costs for the same days
    val dateGroupedDF = adjustedCostDF.groupBy("date").agg(sum("amount").alias("cost"))
    
    //create a new column with the date adjusted to unix standard format to allow correct and uniform processing (e.g. sorting)
    //also sort in descending order of dates to get the last x days
    val datePattern = "dd/MM/yyyy"
    val dateSortedDF = dateGroupedDF.withColumn("timestampCol", unix_timestamp(dateGroupedDF("date"), datePattern).cast("timestamp"))
                                    .sort(desc("timestampCol"))
    
    //extract necessary columns, format the cost to two decimal places and return only the last 60 days
    val last60DaysDF = dateSortedDF.select("date", "cost")
                                   .withColumn("cost", format_number(col("cost"), 2) )
                                   .limit(60)
      
     
     return new Result(name, last60DaysDF)
   }
      
}