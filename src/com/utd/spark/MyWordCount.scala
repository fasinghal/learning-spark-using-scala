package com.utd.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MyWordCount {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.INFO)
    
    val sc = new SparkContext("local[*]", "MyWordCount")
    
    val file = sc.textFile("../SparkScalaMaterial/book.txt")
    
    val words = file.flatMap(line => line.split(" "));
    
    val wordCounts = words.map(x => (x,1)).reduceByKey((x,y)=>(x+y))
    
    wordCounts.collect().sorted.foreach(println)
    
    val topWords = wordCounts.map(x=>x.swap).sortByKey(false, 1).map(x=>x.swap).take(10)
   
    topWords.foreach(println)

  }
  
  
}