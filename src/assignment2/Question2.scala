package assignment2
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object Question2 {
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
	
	def loadUserData(line : String)={
	  val fld = line.split(",")
	  (fld(0).toInt, (fld(1).toString, fld(2).toString, fld(3).toString))
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
		val finalRes = removeNoMutualFrndRows.map(x => (x._1, x._2._2.length))
		
		val topFriends = finalRes.map(line => line.swap).sortByKey(false, 1).map(line =>line.swap).take(10)
		
		//top Ten Mutual Friend
		//topFriends.foreach(println)
		
		//load User Data
		val userData = sc.textFile("../userdata.txt").map(loadUserData)
		
		for(topf <-topFriends){
			    val commonf = topf._2.toInt
					val user1 = userData.lookup(topf._1._1.toInt)(0)
					val user2 = userData.lookup(topf._1._2.toInt)(0)
					val fname1 = user1._1
					val lname1 = user1._2
					val addr1 =user1._3
					val fname2 = user2._1
					val lname2 = user2._2
					val addr2=user2._3
		  
		  println(s"$commonf  $fname1  $lname1  $addr1  $fname2  $lname2  $addr2")	  
		}	
	}
}