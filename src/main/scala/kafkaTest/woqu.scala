package kafkaTest

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object woqu {
  print(1)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |           |  <brokers> is a list of one or more Kafka brokers
           |           |  <topics> is a list of one or more kafka topics to consume from
           |           |
        """.stripMargin)
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local[2]")

    var ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(50))

    val Array(brokers, topics) = args

    val zkQuorum: String = "106.12.10.241:2181"
    val groupId: String = "certusGroup"
    val topicTime: Map[String, Int] = Map[String, Int]("certus" -> 1)

    val topicsSet = topics.split(",").toSet
    val stringToString = Map[String, String]("metadata.broker.list" ->
      brokers,"auto.offset.reset" -> "smallest")
    val kafkastream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, stringToString,
      topicsSet)
    //    val value = KafkaUtils.createDirectStream(,)

//    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
//      val currentCount = values.sum
//
//      val previousCount = state.getOrElse(0)
//
//      Some(currentCount + previousCount)
//    }
//
//    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
//      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
//    }

//    updateFunc(Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]

    var initRdd = ssc.sparkContext.parallelize(List(("hello",1),("world",2)))

//    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
//      val currentCount = values.sum
//
//      val previousCount = state.getOrElse(0)
//
//      Some(currentCount + previousCount)
//    }
//
//    var updateFunc1 = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
//      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
//    }
//    kafkastream.updateStateByKey[Int](updateFunc1,new HashPartitioner(ssc.sparkContext.defaultParallelism),false,
//      initRdd)

    val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约。然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      // 返回累加后的结果。是一个Option[Int]类型
      Some(currentCount + previousCount)
    }

    ssc.checkpoint("hdfs://172.16.3.246:9000")

    val value = kafkastream.map(line => line._2 -> 1)
    val totalWordCounts = value.updateStateByKey[Int](addFunc)

    totalWordCounts.foreachRDD((a,b) => {
      a.foreach(bb => {
        println(bb._1 + "   "+bb._2)
      })
    })

//    kafkastream.foreachRDD((secRdd, b) => {
//      val ranges = secRdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      println(ranges)
//
//      secRdd.foreach(msg => {
//        println(msg._2)
//      })
//
//
//      val tuples = secRdd.map(msg => msg._2 -> 1).reduceByKey(_+_).collect()
//      tuples.foreach(res => {
//        print(res._1 + "   "+ res._2)
//      })
//
//      //如何 迭代 之前的 rdd
//      secRdd
//    })
    ssc.start()
    ssc.awaitTermination()
  }
}
