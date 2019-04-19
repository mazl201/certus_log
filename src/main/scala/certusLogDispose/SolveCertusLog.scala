package certusLogDispose

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

object SolveCertusLog {
  def main(args: Array[String]): Unit = {
    val group = "certusgroup"
    val topic = "certus"

    val sc = new SparkConf().setAppName("certus_log_dispose").setMaster("local[2]")
    val scc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "106.12.10.241",
              "key.deserializer" -> classOf[StringDeserializer],
              "value.deserializer" -> classOf[StringDeserializer],
              "group.id" -> group,
              "auto.offset.reset" -> "earliest",
              "enable.auot.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)

//    val kafkastream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,kafkaParams)




  }
}
