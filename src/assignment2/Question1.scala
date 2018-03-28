package assignment2
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Question1 {
   def parseLine(line: String)= {
    val fields = line.split("\t")
    if (fields.length == 2) {
      val user = fields(0)
      val friendsList = fields(1).split(",").toList
      (user, (1L, friendsList))
    } else {
      ("-1", (-1L, List()))
    }
  }
   
   def main(args: Array[String]) {
     
      Logger.getLogger("org").setLevel(Level.INFO)
      val sc = new SparkContext("local[*]", "Friends")
      
      val mutualRDD = sc.textFile("../soc.txt").map(parseLine).filter(line => line._1!="-1")
      
      val spreadRDD = mutualRDD.flatMap(
      x => x._2._2.map(y => 
        if (x._1.toLong < y.toLong) { ((x._1, y), x._2) } else { ((y, x._1), x._2) }))
     
      val reducedRDD = spreadRDD.reduceByKey((x,y) => (x._1+y._1, x._2.intersect(y._2)))
      val removeNoMutualFrndRows = reducedRDD.filter(x => x._2._1 != 1)
      val finalRes = removeNoMutualFrndRows.map(x => (x._1, x._2._2))
      
      val filterEmptyMutual = finalRes.filter(x => x._2 !=List())
      
      //demo
      filterEmptyMutual.take(5).foreach(println)
      
      //final output
      //finalRes.foreach(println)
   }
}