package cn.com.dwsoft.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2016/4/11.
 */
object ScalaUpdateStateByKeyWC {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ScalaUpdateStateByKeyWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val map: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    map.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val newValue: Int = values.sum
      val oldValue: Int = state.getOrElse(0)
      Some(oldValue + newValue)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
