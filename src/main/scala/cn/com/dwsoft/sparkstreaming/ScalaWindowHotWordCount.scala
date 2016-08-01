package cn.com.dwsoft.sparkstreaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2016/4/15.
 */
object ScalaWindowHotWordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ScalaUpdateStateByKeyWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val searchLogDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    searchLogDStream.map(_.split(" ")(1))
      .map((_, 1))
      .reduceByKeyAndWindow((i1: Int, i2: Int) => i1 + i2, Seconds(15), Seconds(10))
      .transform(
        rdd => {
          val hotWord = rdd.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))

          hotWord.take(3).foreach(x => println())

          hotWord
        }
      ).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
