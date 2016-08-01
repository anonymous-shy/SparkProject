package cn.com.dwsoft.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by root on 2016/3/1.
 */
public class JavaWordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("JavaWordCount");

        /**
         * 创建 JavaStreamingContext 对象，类似 SQLContext ，除了接受一个
         * SparkConf 对象外，还要接受一个 batch interval 参数，即：没收集多长时间的数据
         * 划分为一个 batch 进行处理，这里设置为 1s
         */
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        /**
         * 创建一个输入的 DStream ，代表了一个从数据源(kafka 或 socket)来持续不断的实时数据流，
         * JavaReceiverInputDStream 代表了一个输入的 DStream
         */
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        /**
         * 对接收到的数据执行计算，使用 Spark Core 提供的算子，在 DStream 中执行
         * 在底层，实际上是对DStream中的RDD执行算子形成新的DStream中的RDD
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
