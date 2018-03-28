package myMLLib
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.optimization.SquaredL2Updater
object MyLinearRegression {

	def main(args : Array[String]){
		Logger.getLogger("org").setLevel(Level.ERROR)

		val sc = new SparkContext("local[*]", "Linear Regression")

		val train = sc.textFile("../SparkScalaMaterial/regression.txt")
		              .map(LabeledPoint.parse(_)).cache
		val test = sc.textFile("../SparkScalaMaterial/regression.txt")
		             .map(LabeledPoint.parse(_))

		val lm   = new LinearRegressionWithSGD()
		lm.optimizer
		.setNumIterations(100)
		.setStepSize(1.0)
		.setUpdater(new SquaredL2Updater())
		.setRegParam(0.01)
		
		
		val model = lm.run(train)
		
		val predictions = model.predict(test.map(x=> x.features))
		
		val predAndActual = predictions.zip(test.map(x=>x.label))
	
    for (prediction <- predAndActual) {
      println(prediction)
    }
		
		
		val sqsum = predAndActual.map(row =>Math.pow((row._1.toDouble - row._2.toDouble), 2).toDouble)
		
		val MSE = sqsum.collect().sum / sqsum.collect().length
		
		println("MSE " +MSE)


	}
}  