package com.pyiapa.statcalculator.service

import org.apache.spark.sql.DataFrame

import java.lang.RuntimeException

/**
 * Ensures that every class processing an expenses file
 * (e.g. with columns "date, expense, amount, reimbursed, location" and in String format)
 * is using a file of the correct format
 * 
 * @pyiapa
 * 
 */
trait ExpensesFormatChecker {
  
  final val COLUMN_SIZE = 5
  
  /**
   * Checks if size and format of dataset adheres to the
   * espenses format standards
   * 
   * @param inputDF the dataset in question
   * 
   */
  def checkFormat(inputDF: DataFrame) = {
    
    val columns = inputDF.columns
    
    if(columns.size != COLUMN_SIZE)
      throw new RuntimeException("Incorrect column size in dataframe")
    
    if( !columns.contains("date") || !columns.contains("expense") || !columns.contains("amount") ||
        !columns.contains("reimbursed") || !columns.contains("location") )
      throw new RuntimeException("Incorrect column name found in dataframe")
      
    
    val date = inputDF.select("date").first().get(0)
    
    if(!date.isInstanceOf[String])
      throw new RuntimeException("Incorrect date format")
    
    if (!date.asInstanceOf[String].matches("\\d{2}/\\d{2}/\\d{4}")) {
      throw new RuntimeException("Incorrect date format - Date should be in dd/mm/yyyy format")
    }
    
    val expense = inputDF.select("expense").first().get(0)
    
    if(!expense.isInstanceOf[String])
      throw new RuntimeException("Incorrect expense format")
    
    
    val amount = inputDF.select("amount").first().get(0)
    
    if(!amount.isInstanceOf[String])
      throw new RuntimeException("Incorrect amount data type")
    
    if (!amount.asInstanceOf[String].matches("\\d+.\\d+")) {
      throw new RuntimeException("Incorrect amount format - Amount should be in 00.00 format")
    }
    
    val reimbursed = inputDF.select("reimbursed").first().get(0)
    
   
    if(reimbursed != null && !reimbursed.isInstanceOf[String])
      throw new RuntimeException("Incorrect reimbursed data type")
    
    if (reimbursed != null && !reimbursed.asInstanceOf[String].equalsIgnoreCase("true")) {
      throw new RuntimeException("Incorrect reimbursed format")
    }
    
    val location = inputDF.select("location").first().get(0)
    
    
    if(!location.isInstanceOf[String])
      throw new RuntimeException("Incorrect location data type")
    
    
  }
}