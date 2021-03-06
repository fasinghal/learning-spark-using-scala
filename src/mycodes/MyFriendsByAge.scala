package mycodes

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object MyFriendsByAge {

  def parseLine(line : String ) ={
    
    val values= line.split(",")
    val age = values(2).toInt
    val friends = values(3).toInt
    (age, friends)    
  }
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "MyFriendsAgeTest")
    
    val lines = sc.textFile("../SparkScalaMaterial/fakefriends.csv") // a,b,c,d
    
    val dataRdd = lines.map(parseLine) // (age, friends)
    
    val trsnf = dataRdd.mapValues(x => (x,1))
                .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
                .mapValues(x => x._1/x._2)
    val res = trsnf.collect()
    
    res.sorted.foreach(println)
    //group by key approach 
    
    val secondMethod = dataRdd.groupByKey().mapValues(x =>x.toList.sum/x.toList.length )
     
    val res2 = secondMethod.collect().take(10).foreach(println)
    
    
        
  }
  
  
}