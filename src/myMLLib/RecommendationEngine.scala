package myMLLib
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.mllib.recommendation._
object RecommendationEngine {
  
   def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
   
   def main(args : Array[String]){

	  Logger.getLogger("org").setLevel(Level.ERROR)
	  val sc = new SparkContext("local[*]", "MyALSTest")
	  
	  val nameDict = loadMovieNames()
	  
	  val rdd = sc.textFile("../ml-100k/u.data")
	   
	  val ratings  = rdd.map(x => x.split("\t")).map(x=> new Rating(x(0).toInt, x(1).toInt, x(2).toDouble)).cache()
	  
	  val rank = 8
	  val numIterations = 20
	  
	  val model = ALS.train(ratings, rank, numIterations)
	  
	  val user = 0
	  
	  val userRatings = ratings.filter(x => x.user == user)
	  
	  val myRatings = userRatings.collect()
	  
	  for(line <- myRatings){
	    println(nameDict(line.product.toInt) + " score " + line.rating.toString)
	  }
	  
	  val recommendation = model.recommendProducts(user, 10)
	  
	  for(rec <- recommendation){
	    println(nameDict(rec.product.toInt) + " Score " + rec.rating  )
	  }

	} 
  
}