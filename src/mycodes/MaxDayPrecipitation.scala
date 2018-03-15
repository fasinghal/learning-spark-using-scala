package mycodes

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object MaxDayPrecipitation {
  
  def parseLine(line : String)={
    val fields = line.split(",")
    (fields(0),(fields(1),fields(2),fields(3).toFloat))  //(key , (,.., Values , .. ,))
  }
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "MaxPrecipitation")
    
    val file= sc.textFile("../SparkScalaMaterial/1800.csv")
    
    val rdd = file.map(parseLine)
    
    val rddWithTypeField = rdd.filter(x => x._2._2 == "PRCP")
    
    //stationDatePrecip.take(10).foreach(println)
    
    val stationDatePrecData = rddWithTypeField.mapValues(x => (x._1,x._3))
    
   // stationDatePrecData.take(10).foreach(println)
    
    val res = stationDatePrecData.reduceByKey((x,y) => if(x._2 >= y._2) (x) else (y))
    
    res.collect().sorted.foreach(println)
    
 
    
    
  }
}