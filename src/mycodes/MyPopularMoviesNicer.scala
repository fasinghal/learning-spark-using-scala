package mycodes
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
object MyPopularMoviesNicer {
  
  def loadMovies () : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    
    for(line <- lines ){
      val fields = line.split('|')
      if(fields.length >1){
        movieNames += (fields(0).toInt-> fields(1))
      }
    }
    return movieNames
  }
 
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.INFO)
   
    val sc = new SparkContext("local[*]", "MyPopularMoviesNicer")  
    
    val nameDict = sc.broadcast(loadMovies)
    
    val lines = sc.textFile("../ml-100k/u.data")
     
    val topMovies = lines.map(x => (x.split("\t")(1).toInt, 1))
                           .reduceByKey((x,y)=>x+y)
                           .map(x=>x.swap)
                           .sortByKey(false,1)
                           .take(10)
                           .map(x=>x.swap)
     
     for(mov <-topMovies){
       val title = nameDict.value(mov._1.toInt)
       val movieRating = mov._2
       println(s"$title : $movieRating")       
     }
  }
}