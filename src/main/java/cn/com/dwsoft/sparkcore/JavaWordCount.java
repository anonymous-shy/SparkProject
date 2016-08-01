package cn.com.dwsoft.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by root on 2015/12/21.
 */
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text = sc.textFile("hdfs://10.13.244.41:9000/spark/wc.input");
        /**
         * 使用内部类实现 flatMap 算子
         */
        JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split("\t"));
            }
        });

        /**
         * 实现 map 算子(java中为mapToPair)
         */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * 实现 reduceByKey 算子
         */
        JavaPairRDD<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        /**
         * foreach 打印
         */
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1() + " : " + wordcount._2());
            }
        });

        sc.close();
    }
}
