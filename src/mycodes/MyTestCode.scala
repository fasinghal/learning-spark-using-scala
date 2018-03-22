package mycodes
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MyTestCode {
  def main(args : Array[String]){
    
    
    Logger.getLogger("org").setLevel(Level.INFO)
	val spark = SparkSession
	.builder
	.appName("SparkSQL")
	.master("local[*]")
	.getOrCreate()
	
	val t = List(1.0,2.0,4.0,8.0)
	val a = spark.sparkContext.parallelize(t)
	val b = a.map(x => (x, x*4.0))
	val c = a.map(x => (x, x*2.0))
	val d = a.map(x => (x, x*1.0))
	
	//val e = b.join(c).join(d).foreach(println)
	
	val acc = spark.sparkContext.accumulator(0, "MyAccu")
	val dat = Array(1,2,3,4)
	
	var rdd=spark.sparkContext.parallelize(dat)
	//accumulator wont update unless an Action is called on it
	// eg if we dont call the collect() or foreach etc action , the transformation is 
	//never fired due to lazy evaluation in spark
  //thus accumulator remains at 0
	rdd.map(x => acc+=x)
	println("Value value: "+acc.value)
	
	rdd.map(x => acc+=x).collect().foreach(println)
	println("Value value: "+acc.value)
	
	val tes = List((1, List(2,3,4), (2,List(3,4,5))))
	val tes1 = spark.sparkContext.parallelize(tes)
	val ress = tes1.map(x=> (x._1,x._2.map(t => t*2))).collect()
	
  }
  
}