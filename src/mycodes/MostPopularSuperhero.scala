package mycodes
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
object MostPopularSuperhero { 
  def parseLine (line :String )={
    val fields = line.split("\\s+").toList
    (fields(0).toInt, fields.length-1)
  }
  def loadHeros(line : String) : Option[(Int, String)] ={
      val fields = line.split("\"")
      if(fields.length >1){
        return Some(fields(0).trim().toInt, fields(1).toString)
      }
      else {
        return None
      }
    }
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Most Popular Superhero")
   
    val graph = sc.textFile("../SparkScalaMaterial/Marvel-graph.txt")
    val counts = graph.map(parseLine).reduceByKey((x,y)=>(x+y))
    val topTen = counts.map(x=>x.swap)
                        .sortByKey(false, 1)
                        .map(x=>x.swap).take(10)
    val herosFile = sc.textFile("../SparkScalaMaterial/Marvel-names.txt")
    val heroRdd = herosFile.flatMap(loadHeros)
      
    topTen.foreach(x=>{
      val id = x._1
      val count = x._2
      //lookup and RDD -> returns Seq[String]. Take first Value
      //Lookup on RDD is only avaiable when RDD is strictly of (K,V) form
      val heroName = heroRdd.lookup(id)(0)
      println(s"$heroName has : $count friends")
    })
  }
  
}