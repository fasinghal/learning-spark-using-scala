package mysparksql

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
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
	println( "Direct operations on DataSets avoiding SQL") 

	peopleDS.select("name", "age", "id").orderBy("id").show()
	peopleDS.filter(peopleDS("age")>21).show() // = SELECT * FROM table WHERE 
	peopleDS.filter(peopleDS("name")==="Will").show()

	// default is -> orderBy("count") which is ASC
	peopleDS.groupBy("age").count().orderBy(desc("count")).show() 

	peopleDS.select("name", "age", "id").filter(peopleDS("age")>21).show
	
	//select columns from Table where age > 21 group by age
	peopleDS.select("name", "age", "id").filter(peopleDS("age")>21).groupBy("age").count.show
	
	spark.stop //stop the session  
}

}