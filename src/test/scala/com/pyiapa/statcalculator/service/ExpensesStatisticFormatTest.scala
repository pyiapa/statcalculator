package com.pyiapa.statcalculator.service

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import com.pyiapa.statcalculator.service._
import com.pyiapa.statcalculator.domain._
import com.pyiapa.statcalculator.app._

/**
 * Class to test the format checking functionality
 */
class ExpensesStatisticFormatTest extends FunSuite{
  
  val spark = MainSparkSession.getCurrentSparkSession()
  import spark.implicits._
  
  object FakeImpl extends ExpensesStatistic {
    override def calculate(): Result = {
      null
    }
  }
  
  test("Test dataframe with incorrect column number") {
      
    val df = Seq(
                  ("", "","","", "", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location", "ExtraColumn")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect column size in dataframe")
    
  }
  
  test("Test dataframe with incorrect column names") {
      
    val df = Seq(
                  ("", "","","", "")
                ).toDF("date", "IncorrectColumnName", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect column name found in dataframe")
    
  }
  
  test("Test dataframe with incorrect date format") {
      
    val df = Seq(
                  ("12/12/19", "","","", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect date format - Date should be in dd/mm/yyyy format")
    
  }
  
  test("Test dataframe with incorrect date data type") {
      
    val df = Seq(
                  (121219, "","","", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect date format")
    
  }
  
  test("Test dataframe with incorrect expense data type") {
      
    val df = Seq(
                  ("19/02/2018", 34,"","", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect expense format")
    
  }
  
  test("Test dataframe with incorrect amount format") {
      
    val df = Seq(
                  ("12/12/2018", "abc","33","", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect amount format - Amount should be in 00.00 format")
    
  }
   
  test("Test dataframe with incorrect amount data type") {
      
    val df = Seq(
                  ("19/02/2018", "abc",33.13,"", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect amount data type") 
  }
  
  test("Test dataframe with incorrect reimbursed data type") {
 
    val df = Seq(
                  ("19/02/2018", "abc","33.13",true, "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect reimbursed data type")
    
  }
  
  test("Test dataframe with incorrect reimbursedformat") {
      
    val df = Seq(
                  ("19/02/2018", "abc","33.13","", "")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
     
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect reimbursed format")
    
  }
  
  test("Test dataframe with incorrect location data type") {
      
    val df = Seq(
                  ("19/02/2018", "abc","33.12", null.asInstanceOf[String], 9)
                ).toDF("date", "expense", "amount", "reimbursed", "location")
  
    val thrown = intercept[RuntimeException] {
       FakeImpl.checkFormat(df)
    }
    assert(thrown.getMessage === "Incorrect location data type")
    
  }
  
  
}