package com.pyiapa.statcalculator.service

import org.scalatest.FunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.math.BigDecimal

import com.pyiapa.statcalculator.service._
import com.pyiapa.statcalculator.app._

/**
 * Class to test DailySpending functionality
 * 
 * Note that there is an assumption here that 
 * a reimbursed month accounts to zero cost.
 */
class DailySpendingTest extends FunSuite {
  
  final val EXPECTED_FIRST_ENTRY = "21/08/2019"
  final val EXPECTED_LAST_ENTRY = "10/02/2011"
  final val EXPECTED_SUM_OF_COSTS = 4140.97
  
  val spark = MainSparkSession.getCurrentSparkSession()
  import spark.implicits._
  
  test("Test daily spending of last 60 days") {
    
    val testDF = createNormalTestDataFrame()
    
    val allEntriesDF = new DailySpending(testDF)
    
    val dailySpending60Days = allEntriesDF.calculate()
    
    val resultDF = dailySpending60Days.result
    
    //get the first result (i.e. last day of last 60 days)
    val firstEntry = resultDF.select("date").first().get(0)
    
    val datePattern = "dd/MM/yyyy"
    val dateSortedDF = resultDF.withColumn("timestampCol", unix_timestamp(resultDF("date"), datePattern).cast("timestamp"))
                                    .sort(asc("timestampCol"))
    
    //get the last result (i.e. first day of last 60 days)
    val lastEntry = dateSortedDF.select("date").first().get(0)
    
    val sumDF = resultDF.agg(sum("cost").as("sum"))
    
    val costSum: Double = sumDF.select("sum").first().get(0).asInstanceOf[Double]
    val formattedSum = BigDecimal(costSum).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    
    //check if first entry, last entry and sum of all costs are correct 
    assert(firstEntry === EXPECTED_FIRST_ENTRY)
    assert(lastEntry === EXPECTED_LAST_ENTRY)
    assert(formattedSum === EXPECTED_SUM_OF_COSTS)
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
                   ("10/06/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/05/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/08/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("17/05/2018", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("21/01/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("01/01/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/05/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("13/04/2016", "Apples", "31.66", "true", "England"),
                   ("24/11/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/04/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/02/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/06/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/08/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("22/11/2013", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("22/12/2018", "Campari", "69.11", null.asInstanceOf[String], "Italy"),
                   ("21/05/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("27/04/2013", "Vaccum Bag", "76.3", "true", "Greece"),
                   ("01/05/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/01/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("17/07/2018", "Coffee", "22.56", null.asInstanceOf[String], "Italy"),
                   ("13/08/2016", "Apples", "31.66", "true", "England"),
                   ("24/12/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/04/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/02/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/04/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/03/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("17/08/2018", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("21/01/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("01/02/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/04/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("13/09/2016", "Apples", "31.66", "true", "England"),
                   ("24/12/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/02/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/04/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/06/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/02/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("22/02/2013", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("22/02/2018", "Campari", "69.11", null.asInstanceOf[String], "Italy"),
                   ("21/02/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("27/08/2013", "Vaccum Bag", "76.3", "true", "Greece"),
                   ("01/09/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/11/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("17/03/2018", "Coffee", "22.56", null.asInstanceOf[String], "Italy"),
                   ("13/07/2016", "Apples", "31.66", "true", "England"),
                   ("24/09/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/06/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/03/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/04/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/02/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("17/06/2018", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("21/08/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("01/06/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/03/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("13/07/2016", "Apples", "31.66", "true", "England"),
                   ("24/05/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/03/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/08/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/02/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/07/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("22/01/2013", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("22/01/2018", "Campari", "69.11", null.asInstanceOf[String], "Italy"),
                   ("21/01/2019", "Tea", "60.74", null.asInstanceOf[String], "England"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Greece"),
                   ("01/01/2017", "Fruit", "34.32", null.asInstanceOf[String], "Spain"),
                   ("17/05/2014", "Coffee", "22.56", null.asInstanceOf[String], "Greece"),
                   ("17/05/2018", "Coffee", "22.56", null.asInstanceOf[String], "Italy"),
                   ("13/04/2016", "Apples", "31.66", "true", "England"),
                   ("24/11/2018", "Banana", "22.56", null.asInstanceOf[String], "Greece"),
                   ("10/06/2011", "Wine", "92.53", null.asInstanceOf[String], "Portugal"),
                   ("27/09/2013", "Vaccum Bag", "76.3", "true", "Japan"),
                   ("17/05/2012", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("04/08/2012", "Soap", "89.29", null.asInstanceOf[String], "Brazil"),
                   ("17/05/2018", "Campari", "69.11", null.asInstanceOf[String], "Malta"),
                   ("10/06/2018", "Wine", "92.53", null.asInstanceOf[String], "Italy")
                ).toDF("date", "expense", "amount", "reimbursed", "location")
    
    
    return df
  }
  
}