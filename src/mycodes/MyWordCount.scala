package mycodes

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object MyWordCount {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.INFO)
    
    val sc = new SparkContext("local[*]", "MyWordCount")
    
    val file = sc.textFile("../SparkScalaMaterial/book.txt")
    
    val words = file.flatMap(line => line.split("\\W+"));
    
    val lowerCase = words.map(x => x.toLowerCase())
    
  
    val wordCountsSimple = lowerCase.countByValue()
    
    val stopWords = sc.textFile("../SparkScalaMaterial/enstop.txt")
    val broadCastSW = sc.broadcast(stopWords.collect().toSet)
    val wordminusStopwords = lowerCase.filter(!broadCastSW.value.contains(_))
    
    val wordCounts = wordminusStopwords.map(x => (x,1)).reduceByKey((x,y)=>(x+y))
    
   // wordCounts.collect().sorted.foreach(println)
    
    val topWords = wordCounts.map(x=>x.swap)
                              .sortByKey(false, 1)
                                .map(x=>x.swap)
                                 
    topWords.collect().take(10).foreach(println)
    
    
  }
 
  
}