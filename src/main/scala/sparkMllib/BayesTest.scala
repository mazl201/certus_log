package sparkMllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

object BayesTest {

  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Bayes").setMaster("local[2]")

//    val sc = new SparkContext(conf)

    val sparkSql = SparkSession.builder()
      .config(conf).getOrCreate()

    val data = sparkSql.read.format("libsvm").load("")

    val Array(trainingData,testData) = data.randomSplit(Array(0.7,0.3),seed=1234L)

    val model = new NaiveBayes().fit(trainingData)

    val predictions = model.transform(testData)

    predictions.show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")


    val accuracy = evaluator.evaluate(predictions)

    println("Accuracy:"+accuracy)
  }
}
