package kafkaTest

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object woqu2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("textStreamTest").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(5))

    context.fileStream("D:\\Certus\\log\\spring.log")



//    context.textFileStream()

  }
}
