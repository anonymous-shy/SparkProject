package shy.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2016/4/15.
 */
object ScalaTransformBlackList {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScalaTransformBlackList")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val blackListRdd: RDD[(String, Boolean)] =
      sc.parallelize(List("Shy", "Anonymous"), 5).map((_, true))
    val adsClickLogDStream: ReceiverInputDStream[String] =
      ssc.socketTextStream("localhost", 9999)
    val userAdsClickLogDStream: DStream[(String, String)] = adsClickLogDStream.map(log => (log.split(" ")(1), log))
    val validAdsClickLogDStream: DStream[String] = userAdsClickLogDStream.transform(userRdd => {
      userRdd.leftOuterJoin(blackListRdd)
        .filter(x => x._2._2 != Option(true))
        .map(x => x._2._1)
    })
    validAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
