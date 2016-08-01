package cn.com.dwsoft.sparkstreaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2016/4/19.
 */
object ScalaKafkaReceiver {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ScalaKafkaReceiver")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    /**
     * Create DStream from Kafka
     * Create an input stream that pulls messages from Kafka Brokers.
     * ssc       StreamingContext object
     * zkQuorum  Zookeeper quorum (hostname:port,hostname:port,..)
     * groupId   The group id for this consumer
     * topics    Map of (topic_name -> numPartitions) to consume. Each partition is consumed
     * in its own thread
     * storageLevel  Storage level to use for storing the received objects
     * (default: StorageLevel.MEMORY_AND_DISK_SER_2)
     *
     * Return : ReceiverInputDStream[(String, String)] 返回值中包含(K, V)元组对,一般直接获取Value
     */
    val kafkaReceiverDStream: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, "zk:2181", "topic-group", Map("topic1" -> 1))
    kafkaReceiverDStream.map(_._2)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
