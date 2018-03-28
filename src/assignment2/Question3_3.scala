package assignment2
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
object Question3_3 {
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

def parseGenre(line : String)={
		val values = line.split(",")
				val movieId = values(0).toInt
				// split by any non word character
				val genres = values(2).toString.split("\\W+").toList 
				(movieId,genres)
}

def main(args : Array[String]){
	Logger.getLogger("org").setLevel(Level.ERROR)
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

	val actionMoviesDF =  ratingDS.join(actionTagDS, "movieId").toDF()

	//read and parse genre file
	val genreData = spark.sparkContext.textFile("../movies.csv")
	val genreHeader = genreData.first()
	val genreRDD = genreData.filter(_!=genreHeader).map(parseGenre)
	
	val thrillGenreDS = genreRDD.filter(row => row._2.contains("Thriller")).toDF("movieId", "genres")
	
	val thrillActionMoviesDF = actionMoviesDF.join(thrillGenreDS, "movieId")
	
	//thrillActionMovies.show()
	thrillActionMoviesDF.groupBy("movieId").avg("rating").orderBy("movieId").show()
}
}