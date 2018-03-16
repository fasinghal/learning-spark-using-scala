package mysparksql

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._
import scala.io.Source

object MyDataSetsPopularMovie {
  
  
  def loadMovies(): Map[Int, String] = {
    
    var movieMap : Map[Int, String] = Map()
     implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val movies = Source.fromFile("../ml-100k/u.item").getLines
    
    for(mov <- movies){
      val fld = mov.split('|')
      if(fld.length >1){
        movieMap += (fld(0).toInt -> fld(1).toString)
      } 
    }
    
    return movieMap
  }
  
  case class Movie(movieId : Int)
  
  def main(args : Array[String]){
     Logger.getLogger("org").setLevel(Level.INFO)
     
     val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()
      
    val movieData = spark.sparkContext.textFile("../ml-100k/u.data").map(line => Movie(line.split("\t")(1).toInt))
    
    import spark.implicits._
    val movieDS = movieData.toDS()
    
    val moviesCountsDS = movieDS.groupBy("movieId").count().orderBy(desc("count"))
    
    // take gives back an Array[Row] 
    val topTen = moviesCountsDS.take(10) 
    
    val movieMap = loadMovies()
    
    for(movRow <-topTen){
      println(movieMap(movRow(0).asInstanceOf[Int]) + ":" + movRow(1))
    }
  }
}