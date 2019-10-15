package com.pyiapa.statcalculator.service

import com.pyiapa.statcalculator.domain.Result
import com.pyiapa.statcalculator.app.MainSparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

import java.text.DateFormatSymbols

 
/**
 * Calculates the costliest month
 * 
 * Note that there is an assumption here that 
 * a reimbursed month accounts to zero cost.
 * 
 * @author pyiapa
 * 
 */
class CostliestMonth(val inputDF: DataFrame) extends ExpensesStatistic{
  
   name = "costliest_month"
   
   /**
    * Computes the costliest month over all years
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
     
     //correct the costs - whenever there is a reimbursement replace cost with zero
     val correctCostDF = inputDF.withColumn("amount", when(col("reimbursed").equalTo("true"), 0).otherwise(col("amount") ) )
     
     //date format in our input
     val datePattern = "dd/MM/yyyy"
     
     //convert date to standard unix timestamp to ensure uniform processing of day, month, year later
     val dateFormattedDF = correctCostDF.withColumn("timestampCol", unix_timestamp(correctCostDF("date"), datePattern).cast("timestamp"))
     
     //breakdown the date into separate columns of month and year 
     val dateExtractDF = dateFormattedDF.withColumn("month", month(col("timestampCol"))).withColumn("year", year(col("timestampCol")))
     
     //retain only necessary columns (in this case we are interested only in month, year, and amount)
     val selectedFieldsDF = dateExtractDF.select("amount", "month", "year")
     
     //register dataframe as a table name to allow SQL statements like in a relational Database
     selectedFieldsDF.createOrReplaceTempView("costs")
     
     //aggregate entries by month and year, sum up the entries with the same (month,year) and order the result in descending order
     val aggrDF = MainSparkSession.getCurrentSparkSession().sql("SELECT month, year, SUM(amount) AS cost FROM costs GROUP BY month, year ORDER BY cost DESC")
     
     //user-defined function to extract textual version of a numeric month (e.g. 02 == February)
     val getMonthUDF = udf(getMonth)
     
     //select only the top entry of the result (i.e. the costliest month in all years) and replace numeric months with their textual version
     val result = aggrDF.limit(1)
                        .withColumn("cost", format_number(col("cost"), 2) ).withColumn("month", getMonthUDF(col("month")))
    
     
     return new Result(name, result)
   }
   
   /**
    * Convert a numeric month to the textual equivalent
    */
   def getMonth: (Int => String) = { 
     month => new DateFormatSymbols().getMonths()(month - 1) 
   }
   
      
}