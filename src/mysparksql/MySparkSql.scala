package mysparksql

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object MySparkSql {
  
  case class Person(id : Int , name : String, age: Int, numOfFriends : Int)
  def parseLine(line:String) ={
    
    val fields = line.split(",")
    val person : Person = new Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }
  
  def main(args : Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
      
      val file = spark.sparkContext.textFile("../SparkScalaMaterial/fakefriends.csv")
      val people = file.map(parseLine)
      
      import spark.implicits._
      val peopleDS = people.toDS
      
      peopleDS.printSchema()
      
      
      // show() is available to both DataSets and DataFrames
      // by default show() displays only top 20 rows
      // to show n rows, call show(n)
      peopleDS.show(30)
      
      peopleDS.createOrReplaceTempView("people")
      
      val teens  = spark.sql("SELECT * FROM people WHERE age>=16 AND age <=19")
      
      //print as rdd
     teens.rdd.foreach(println)
    
      //print Data Frame
     teens.show()
      
      
      
      println( "Direct operations on Datasets avoiding SQL") 
      
      peopleDS.select("name").orderBy("name").show()
      peopleDS.filter(peopleDS("age")>21).show()
      peopleDS.groupBy("age").count().orderBy("age").show()
  
      spark.stop() //stop the session  
  }
  
}