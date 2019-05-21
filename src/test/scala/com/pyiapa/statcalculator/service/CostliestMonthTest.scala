package com.pyiapa.statcalculator.service

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import com.pyiapa.statcalculator.service._
import com.pyiapa.statcalculator.app._

/**
 * Class to test CostliestMonth functionality.
 * Note that there is an assumption here that 
 * a reimbursed month accounts to zero cost.
 */
class CostliestMonthTest extends FunSuite {
  
  final val EXPECTED_MONTH = "June"
  
  final val EXPECTED_YEAR = 2011
  
  val spark = MainSparkSession.getCurrentSparkSession()
  import spark.implicits._
  
  test("Test costliest month") {
    
    val testDF = createNormalTestDataFrame()
    
    val monthsDF = new CostliestMonth(testDF)
    
    val costliestMonthDF = monthsDF.calculate()
    
    val costliestMonth = costliestMonthDF.result.select("month").first().get(0)
    val yearOfCostliestMonth = costliestMonthDF.result.select("year").first().get(0)
    
    
    assert(costliestMonth === EXPECTED_MONTH)
    assert(yearOfCostliestMonth === EXPECTED_YEAR)
    
  }
  
  def createNormalTestDataFrame(): DataFrame = {
    
    val df = Seq(
                   ("21/01/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("01/01/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/05/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("13/04/2016", "Apples", "31.66", "true", "England"),
                   ("24/11/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/06/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/05/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/08/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("22/01/2013", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("22/01/2018", "Campari", "69.11", null.asInstanceOf[String], "Italy"),
                   ("21/01/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Greece"),
                   ("01/01/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/05/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("17/05/2018", "Coffee", "22.56", null.asInstanceOf[String], "Italy"),
                   ("13/04/2016", "Apples", "31.66", "true", "England"),
                   ("24/11/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("11/06/2011", "Wine", "92.89", null.asInstanceOf[String], "Portugal"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/05/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/08/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("17/05/2018", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("10/06/2018", "Wine", "92.53", null.asInstanceOf[String], "Italy")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
    
    
    return df
  } 
  
  
}