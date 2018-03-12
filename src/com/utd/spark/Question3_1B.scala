package com.utd.spark
import org.apache.spark._
import org.apache.spark.SparkContext._

object Question3_1B{
  def parseLine(line : String)={
    val values = line.split(",")
    val movieId = values(1)
    val rating = values(2).toDouble
    (movieId, rating)
  }
  
  def main(args : Array[String]){
    val sc = new SparkContext("local[*]", "Question3_1A")
    val input = sc.textFile("../ratings.csv")
    val header = input.first()
    val data = input.filter(_!=header)
    val rdd = data.map(parseLine)
                                .mapValues(rating => (rating,1))
                                .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
                                .mapValues(x => x._1 / x._2)
                                
    val leastTen = rdd.map(item => item.swap)
                                .sortByKey(true,1)
                                .map(item => item.swap)
                                .collect()
                                .take(10)
                             
    leastTen.foreach(println)
  }
}