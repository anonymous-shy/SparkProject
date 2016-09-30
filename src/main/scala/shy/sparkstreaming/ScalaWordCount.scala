package shy.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by root on 2016/3/1.
 */
object ScalaWordCount extends App {

  val conf = new SparkConf().setMaster("local[2]").setAppName("ScalaWordCount")
  val ssc = new StreamingContext(conf, Seconds(1))
  private val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
  lines.flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)
    .print()
  ssc.start()
  ssc.awaitTermination()

}
