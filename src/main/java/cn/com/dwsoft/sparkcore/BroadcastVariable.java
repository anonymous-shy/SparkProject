package cn.com.dwsoft.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 2015/12/24.
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final int factor = 3;
        final Broadcast<Integer> broadcastValue = sc.broadcast(factor);
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(list);
        JavaRDD<Integer> map = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                return i * broadcastValue.value();
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
