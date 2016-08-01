package cn.com.dwsoft.sparkstreaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 2016/4/11.
 * 基于UpdateStateByKey的实时缓存的WordCount
 */
public class UpdateStateByKeyWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setMaster("local[2]").setAppName("UpdateStateByKeyWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //通过sc定义jssc
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

        //要使用updateStateByKey必须开启checkpoint机制,以防数据丢失
        jssc.checkpoint("hdfs://xxx/yyy/zzz");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaPairDStream<String, Integer> PairDStream = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //Optional类似为Scala中的样例类 Option,代表一个值的存在状态,可能存在或不存在
        JavaPairDStream<String, Integer> updateStateByKey = PairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            /**
             * List<Integer> values, Optional<Integer> state
             * values : 每个Key的新的值,List类型
             * state : Key之前的状态,泛型是自己指定的
             */
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //1.定义全局单词计数
                Integer newValue = 0;
                //2.判断state是否存在，如果不存在，说明这个key第一次出现。
                //如果存在，则说明这个key之前已经统计过全局的次数了
                if (state.isPresent()) {
                    newValue = state.get();
                }
                //3.将本次新出现的值，都累加到newValues上，就是这个key目前为止的全局统计。
                for (Integer value : values)
                    newValue += value;
                return Optional.of(newValue);
            }
        });
        updateStateByKey.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
