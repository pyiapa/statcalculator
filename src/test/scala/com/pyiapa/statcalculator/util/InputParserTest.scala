package com.pyiapa.statcalculator.util

import org.scalatest.FunSuite
import com.pyiapa.statcalculator.util._


/**
 * Includes various tests for the InputParser object
 * 
 * @author pyiapa
 * 
 */
class InputParserTest extends FunSuite{
  
  //final val SMALL_INPUT = "../statcalculator/src/test/resources/input/input_small.csv"
  final val SMALL_INPUT_NAME = "input_small.csv"
  final val SMALL_INPUT_PATH ="/input/" + SMALL_INPUT_NAME
  final val SMALL_INPUT_SIZE = 65
  final val SMALL_INPUT_FIRST_COUNTRY = "Bolivia"
  
  test("Trying to load a missing file should produce IllegalArgumentException") {
    
    val thrown = intercept[IllegalArgumentException] {
      InputParser.loadCSVToDF("incorrect path", "incorrect name")
     
    }
    assert(thrown.getMessage === "Incorrect file path")
   
  }
  
  test("Testing correct input row count") {
   
     val df = InputParser.loadCSVToDF(SMALL_INPUT_PATH, SMALL_INPUT_NAME)
     assert(df.count().toInt == SMALL_INPUT_SIZE)
  }
  
  test("Testing correct input content") {
   
     val df = InputParser.loadCSVToDF(SMALL_INPUT_PATH, SMALL_INPUT_NAME)
     
     val result = df.select("location").first().get(0)
     
     assert(result == SMALL_INPUT_FIRST_COUNTRY) 
  }
  
}