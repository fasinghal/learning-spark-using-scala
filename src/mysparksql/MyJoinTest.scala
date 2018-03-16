
//success !! :D ~( *o*)~
package mysparksql
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MyJoinTest {


	def main(args : Array[String]){

		Logger.getLogger("org").setLevel(Level.INFO)

		val spark = SparkSession
		.builder
		.appName("MySparkSQL")
		.master("local[*]")
		.getOrCreate()


		val sc = spark.sparkContext

		import spark.implicits._
		val employees = sc.parallelize(
				Array[(String, Option[Int])](
						("Rafferty", Some(31)),
						("Jones", Some(33)), 
						("Heisenberg", Some(33)), 
						("Robinson", Some(34)), 
						("Smith", Some(34)), 
						("Williams", null))
				).toDF("LastName", "DepartmentID")

		val departments = sc.parallelize(Array(
				(31, "Sales"), (33, "Engineering"), (34, "Clerical"),
				(35, "Marketing")
				)).toDF("DepartmentID", "DepartmentName")


		//employees.show()
		//departments.show()

		val innerjoin = employees.join(departments, "DepartmentID")

		innerjoin.show()

		val leftouterjoin = employees.join(departments, Seq("DepartmentID"), "left_outer")

		leftouterjoin.show()

		//In 2.x, spark has added an expilict check for cartersian product. 
		//By default all the joins reject the cross product. 
		//So if you run the same code in 2.x, you will get below error.
		//org.apache.spark.sql.AnalysisException
		//So if you really want to have cross join, you need to be explicit in the code
		val cartesianjoin = employees.crossJoin(departments)

		cartesianjoin.show(10)
	}

}