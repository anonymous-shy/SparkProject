package cn.com.dwsoft.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * Created by root on 2016/4/15.
 */
public class WindowHotWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]").setAppName("WindowHotWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

        JavaReceiverInputDStream<String> searchLogDStream = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> searchWordDStream = searchLogDStream.map(new Function<String, String>() {
            @Override
            public String call(String serachLog) throws Exception {
                return serachLog.split(" ")[1];
            }
        });
        JavaPairDStream<String, Integer> searchWordPairDStream = searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String searchWord) throws Exception {
                return new Tuple2<String, Integer>(searchWord, 1);
            }
        });
        //针对{searchWord, 1}tuple格式DStream，执行reduceByKeyAndWindow操作
        JavaPairDStream<String, Integer> windowDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
            /**
             * 第二个参数是窗口长度，1min
             * 第三个参数是滑动间隔，30s
             * 即 每隔30s将最近1min的数据作为一个窗口，进行一个内部聚合，然后统一对RDD进行后续计算
             * 但是 不会立即计算，等到时间间隔到了下个30s的时会将前1min的Rdd(5s一个batch 1min为12个Rdd)聚合起来统一
             *      执行reduceByKey操作，故reduceByKeyAndWindow是针对窗口的而不是DStream中的Rdd
             */
        }, Durations.minutes(1), Durations.seconds(30));
        windowDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                //进行sortByKey操作
                JavaPairRDD<String, Integer> sortedHotWordRdd = pairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<Integer, String>(tuple2._2(), tuple2._1());
                    }
                }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                        return new Tuple2<String, Integer>(tuple._2(), tuple._1());
                    }
                });
                //取搜索前三的热词输出
                List<Tuple2<String, Integer>> hotWordsTop3 = sortedHotWordRdd.take(3);
                for (Tuple2<String, Integer> hotWord : hotWordsTop3) {
                    System.out.println("search word: " + hotWord._1() + ",search times: " + hotWord._2());
                }
                return sortedHotWordRdd;
            }
        }).print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
