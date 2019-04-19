package mysqlTask

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileRepairTask {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      System.err.println(
        s"""
          |Usage: DirectKafkaWordCount <brokers> <topics>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |
        """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("mysqlSpark").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(50))
    val flume = FlumeUtils.createStream(ssc,"172.16.18.148",8099)

    flume.foreachRDD(row => {
      print(row)
    })

    ssc.start()

    ssc.awaitTermination()
  }
}
