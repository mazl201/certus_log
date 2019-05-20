package SimpleCaptureErrorLog

import java.util

import com.mongodb.{BasicDBObject, DBObject}
import com.mongodb.async.client.{Observer, Subscription}
import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.mongodb.scala.{Completed, MongoClient, Observable}
import org.mongodb.scala.bson.collection.immutable.Document

object CaptureErrorLogCollector {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("textStreamTest").setMaster("local[2]")
    val sqlSession = SparkSession.builder
      .config("spark.mongodb.output.uri", "mongodb://172.16.3.247:27017")
      .config("spark.mongodb.output.database","certus_log")
      .config("spark.mongodb.output.collection","error_log").config(conf)
      .getOrCreate()
    val ssc = new StreamingContext(sqlSession.sparkContext, Seconds(20))

//    MongoSpark.load(sqlSession)
//    val value = sqlSession.sparkContext.parallelize(Array(new BasicDBObject("aaa","bbb")))
    val value = new util.ArrayList[BasicDBObject]()
    value.add(new BasicDBObject("aaa","bbb"))
    val value1 = JavaSparkContext.fromSparkContext(sqlSession.sparkContext).parallelize(value)

    MongoSpark.save(value1)


    val stringToString = Map[String, String]("metadata.broker.list" ->
      "106.12.10.241:9092", "auto.offset.reset" -> "smallest")

    val topics = Set("basic-log")




    val kafkastream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, stringToString,
      topics)

    //初始化 一个 stringbuffer
    var noneOrAll: StringBuffer = new StringBuffer

    kafkastream.foreachRDD((a, b) => {

      a.foreach(bb => {
        println(bb._1 + "   " + bb._2)

        val maybeBuffer = new StringBuffer
        var maybeNextLine = ""
        //发现 错误 日志 开始 如果 noneOrAll 为空 且 包含 error 或者 noneOrAll 为空
        if (StringUtils.isNotBlank(bb._2) && (noneOrAll.length() > 0 || (noneOrAll.length() == 0 && bb._2.contains
        ("ERROR")))) {
          maybeNextLine = bb._2
        }
        /*
        判断 日志 结束 如果 日志 不为空 但是 又到了 正常日志的地方 或者 下一个 错误的地方
        */
        //判断 next  是否 是 正常日志
        if (maybeNextLine.contains("certus") && maybeNextLine.contains("@") && maybeNextLine.contains("@@") &&
          !maybeNextLine.contains("ERROR")) {
          //目前 不做 任何 处理 但是 判断 noneOrAll 是否 为空
          //将 相关 日志行 存储 为一个 mongod 数据
          if (StringUtils.isNotBlank(maybeNextLine.toString)) {
            //剪取 第一部分 为简要 brief
            var brief = noneOrAll.substring(0, 100)
            //时间
            val time = brief.trim.split(" ")(1)

            val document = Document("time" -> time, "brief" -> brief, "content" -> maybeBuffer.toString)
            val mongoClient = MongoClient("mongodb://172.16.3.247:27017")
            val certusLogDb = mongoClient.getDatabase("certus_log")
            val collectionClient = certusLogDb.getCollection("error_log")
            val value: Observable[Completed] = collectionClient.insertOne(document)

            value.subscribe(new Observer[Completed] {
              override def onComplete(): Unit = {
                println("insert alice completed.")
              }

              override def onNext(result: Completed): Unit = {
                println("insert alice sucessful.")
              }

              override def onError(e: Throwable): Unit = println(s"insert error: ${e.getMessage}")

              override def onSubscribe(subscription: Subscription): Unit = println("subscribe success")
            })
            //清空 noneOrAll
            noneOrAll = new StringBuffer
          }
        } else {
          //append 然后 继续 加入
          noneOrAll.append(maybeNextLine)
          maybeNextLine = ""
        }
        //分发 测试 生产


        //回传 mongo id 拼装成 错误 日志 左侧 列表 简要 时间


        //是否 全部 存入 到 一个临时 变量中 直到 下一个 有效数据行 到达，然后 进行存储


        //判断是否 需要 存入 并且 清空


      })
    })

    ssc.start()

    ssc.awaitTermination()

  }


}
