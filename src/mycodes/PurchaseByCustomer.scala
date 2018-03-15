package mycodes

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PurchaseByCustomer {
  
  def parseLine(line : String) ={
    
    val fields = line.split(",")
    (fields(0), fields(2).toFloat)  
    
  }
  
  def main(args : Array[String]){
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")   
    
    val file = sc.textFile("../SparkScalaMaterial/customer-orders.csv")
    
    val rdd = file.map(parseLine).reduceByKey((x,y)=> (x+y)).sortByKey(true,1)
    
    //printing
    rdd.collect().foreach( x => {
      val first = x._1
      val temp = x._2
      val second = f"$temp%.2f"
      println(s" $first : $second ") 
    })
    
    //or
    
    /* for(x <- rdd.sorted){
      val first = x._1
      val temp = x._2
      val second = f"$temp%.2f"
      println(s" $first : $second ") 
      
    }*/
        
    //top 10 spender
    
    val sortval = rdd.map(line => line.swap)
                    .sortByKey(false, 1)
                    .map(item => item.swap)
                    .collect().take(10)
    sortval.foreach(println)
  }
  
  
}