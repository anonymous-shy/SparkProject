package cn.com.dwsoft.sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 基于Kafka Direct方式
 * Created by root on 2016/3/8.
 */
public class KafkaDirectWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         *  将 Kafka 参数存入 map 中
         */
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                "10.13.244.41:9092,10.13.244.40:9092,10.13.244.43:9092");
        /**
         *  将 Topic 存入 set 中
         */
        Set<String> topics = new HashSet<String>();
        topics.add("WordCount");
        /**
         * 对应参数分别为 jssc,keyClass,valueClass,keyDecoder,valueDecoder,kafkaParams,topics
         */
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2().split(" "));
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCount.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();
    }
}
