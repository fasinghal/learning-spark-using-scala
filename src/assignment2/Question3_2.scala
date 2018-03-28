package assignment2

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._


object Question3_2 {
case class MovieTag(movieId : Int, tag : String)
case class MovieRating(movieId: Int, rating : Double)
def parseRatings(line : String)={
		val values = line.split(",")
				val movieId = values(1).toInt
				val rating = values(2).toDouble
				MovieRating(movieId, rating)
}
def parseTags(line : String)={
		val values = line.split(",")
				val movieId = values(1).toInt
				val tag = values(2).toString
				MovieTag(movieId,tag)
}

def main(args : Array[String]){
	Logger.getLogger("org").setLevel(Level.INFO)
	val spark = SparkSession
	.builder
	.appName("SparkSQL")
	.master("local[*]")
	.getOrCreate()

	import spark.implicits._
	val ratingRDD = spark.sparkContext.textFile("../ratings.csv")
	val ratingHeader = ratingRDD.first()
	val ratingDS = ratingRDD.filter(_!=ratingHeader).map(parseRatings).toDS

	val tagRDD = spark.sparkContext.textFile("../tags.csv")
	val tagHeader = tagRDD.first()
	val tagDS = tagRDD.filter(_!=tagHeader)
	.map(parseTags).toDS()
	
	val actionTagDS = tagDS.filter(tagDS("tag")==="action")
	actionTagDS.show()
	
	val actionMovies =  ratingDS.join(actionTagDS, "movieId")

  actionMovies.groupBy("movieId").avg("rating").orderBy("movieId").show()
}
}