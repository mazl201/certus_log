package kafkaTest

import kafka.serializer.StringDecoder
import kafkaTest.utils.LogEntity
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object woqu2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("textStreamTest").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(50))

    val stringToString = Map[String, String]("metadata.broker.list" ->
      "106.12.10.241:9092","auto.offset.reset" -> "smallest")

    val topics = Set("certuslogflume")

    val kafkastream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, stringToString,
      topics)

    kafkastream.foreachRDD((a,b) => {
      a.foreach(bb => {
        println(bb._1 + "   "+bb._2)
        val entity = LogEntity.splitStrToLogEntity(bb._2)
      })
    })

    ssc.start()

    ssc.awaitTermination()

  }
}
