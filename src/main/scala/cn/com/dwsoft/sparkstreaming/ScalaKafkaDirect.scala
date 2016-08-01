package cn.com.dwsoft.sparkstreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2016/4/20.
 */
object ScalaKafkaDirect {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ScalaKafkaReceiver")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    /**
     * Create an input stream that directly pulls messages from Kafka Brokers
     * without using any receiver. This stream can guarantee that each message
     * from Kafka is included in transformations exactly once (see points below).
     *
     * Points to note:
     * - No receivers: This stream does not use any receiver. It directly queries Kafka
     * - Offsets: This does not use Zookeeper to store offsets. The consumed offsets are tracked
     * by the stream itself. For interoperability with Kafka monitoring tools that depend on
     * Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
     * You can access the offsets used in each batch from the generated RDDs (see
     * [[org.apache.spark.streaming.kafka.HasOffsetRanges]]).
     * - Failure Recovery: To recover from driver failures, you have to enable checkpointing
     * in the [[StreamingContext]]. The information on consumed offset can be
     * recovered from the checkpoint. See the programming guide for details (constraints, etc.).
     * - End-to-end semantics: This stream ensures that every records is effectively received and
     * transformed exactly once, but gives no guarantees on whether the transformed data are
     * outputted exactly once. For end-to-end exactly-once semantics, you have to either ensure
     * that the output operation is idempotent, or use transactions to output records atomically.
     * See the programming guide for more details.
     *
     * ssc StreamingContext object
     * kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
     * configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
     * to be set with Kafka broker(s) (NOT zookeeper servers), specified in
     * host1:port1,host2:port2 form.
     * If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
     * to determine where the stream starts (defaults to "largest")
     * topics Names of the topics to consume
     *
     * create Kafka DStream from Direct API
     */
    val kafkaParams = Map("metadata.broker.list" -> "storm1:9092,storm2:9092,storm3:9092,storm4:9092,storm5:9092")
    val topics = Set("topic1")
    val kafkaDirectDStream: InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    kafkaDirectDStream.map(_._2)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
