package cn.com.dwsoft.kafka

import java.util
import java.util.{UUID, Date, Properties}

import kafka.consumer.{KafkaStream, ConsumerConnector, Consumer, ConsumerConfig}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection

/**
 * Created by root on 2016/4/19.
 */
object KafkaTest extends App {


}

/**
 * Kafka Producer
 */
object MyKafkaProducer {

  def produceData = {
    //存储kafka属性文件
    val props = new Properties()
    props.put("metadata.broker.list", "broker1:9092,broker2:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("value.serializer.class", "kafka.serializer.StringEncoder")
    //    props.put("partitioner.class", "example.producer.SimplePartitioner")
    props.put("request.required.acks", "1");
    //创建Kafka ProducerConfig
    val config = new ProducerConfig(props)
    //create Producer instance
    val producer: Producer[String, String] = new Producer[String, String](config)
    //def this(topic: String, key: K, message: V) 主题, partition, message
    val message = new KeyedMessage[String, String]("topic1", new Date().getTime.toString, UUID.randomUUID().toString)
    producer.send(message)
    producer.close()
  }
}

/**
 * Kafka Consumer
 */
object MyKafkaConsumer {

  val props = new Properties()
  props.put("zookeeper.connect", "zk:2181") //zk连接
  props.put("group.id", "topic1_group1") //所属consumer group
  props.put("zookeeper.session.timeout.ms", "1000")
  //zk最大连接超时
  private val config: ConsumerConfig = new ConsumerConfig(props)
  private val consumerConnector: ConsumerConnector = Consumer.create(config)
  private val topicMessageStreams: collection.Map[String, List[KafkaStream[Array[Byte], Array[Byte]]]] =
    consumerConnector.createMessageStreams(Map("topic1" -> 1)) //Map("topic1" -> 1) Map中内容为(主题, 分区数)
  topicMessageStreams.values.foreach(list => {
    list.foreach(stream => {
      val iterator = stream.iterator()
      while (iterator.hasNext()) {
        val meg = iterator.next()
        println("===> " + meg.message() + " ===> " + meg.key())
      }
    })
  })
}