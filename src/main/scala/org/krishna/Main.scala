package org.krishna

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  /* Question 1 Solution Total Number of Flights Per month  */
  def findFlightCountPerMonth(flightData: DataFrame): Unit = {
    val filteredData = flightData.groupBy(month(col("date"))).count().orderBy("month");
    filteredData.show(12)
  }

  /* Question 2 Solution Find The Names of 100 most frequent flyers */
  def findMostFrequentFlyers(flightData: DataFrame, pasData: DataFrame, recCount: Int): Unit = {
    val filteredData = flightData.groupBy("passengerId").count()
    val res = filteredData.join(pasData, "passengerId").select("passengerId", "count", "firstName", "lastName").orderBy(desc("count"))
    res.show(recCount)
  }

  /* Question 4 Solution Find The Passengers more than 3 flights s */
  def findCoTravelledPassengers(flightData: DataFrame, flightCount: Int): Unit = {
    val fd2 = flightData.as("fd2")
    val filteredRes = flightData.as("fd1").join(fd2, flightData("passengerId") <=> fd2("passengerId")
      && flightData("flightId") === fd2("flightId") && flightData("date") === fd2("date") &&
      flightData("from") === fd2("from") &&
      flightData("to") === fd2("to")).groupBy(flightData("passengerId"), fd2("passengerId")).count().where(col("count") > 3)
    filteredRes.show(100)
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName("Krishna").master("local[4]").getOrCreate();
    val flightData = ss.read.option("header", "true").csv("flightData.csv")
    val pasData = ss.read.option("header", "true").csv("passengers.csv")

    /* Question 1  */
    findFlightCountPerMonth(flightData)

    /* Question 2  */
    findMostFrequentFlyers(flightData, pasData, 100)

    /* Question 4  */
    findCoTravelledPassengers(flightData, 3)
}
}
