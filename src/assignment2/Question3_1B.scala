package assignment2
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

object Question3_1B{
  def parseLine(line : String)={
    val values = line.split(",")
    val movieId = values(1).toInt
    val rating = values(2).toDouble
    (movieId, rating)
  }
  
  def loadMovies() : Map[Int, String]={
    
    var movieMap : Map[Int, String] = Map()
    val movieData = Source.fromFile("../movies.csv").getLines()
                                                    .drop(1) //removing the header   
    
    for(line <- movieData){
      val fields = line.split(',')
      movieMap += (fields(0).toInt-> fields(1).toString)      
    }
    return movieMap
  }
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Question3_1B")
   
    val input = sc.textFile("../ratings.csv")
    val movieMap = sc.broadcast(loadMovies())
    
    val header = input.first()
    val data = input.filter(_!=header)
    
    val rdd = data.map(parseLine)
          .mapValues(rating => (rating,1)) // (movie (rating, n))
             .reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
               .mapValues(x => x._1 / x._2)
                            
                //(movieId, AvgRating) -> (AvgRating, movieId)
    val leastTen = rdd.map(item => item.swap)
                       .sortByKey(true,1) // sort by avg Rating in ASC order
                       .map(item => item.swap) // flip again =>(movieId, AvgRating)
                       .take(10) // take 10 least rated movies
    val result = leastTen.map(x => (movieMap.value(x._1.toInt), x._2))
    
    for(value <-result){
      println(value._1 , value._2)
    }
   
  }
}