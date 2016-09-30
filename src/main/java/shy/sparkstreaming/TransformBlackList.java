package shy.sparkstreaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * transform 实现黑名单过滤
 * Created by root on 2016/4/14.
 */
public class TransformBlackList {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]").setAppName("UpdateStateByKeyWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

        //1.模拟黑名单数据RDD
        final List<String> blackList = Arrays.asList("Shy", "Anonymous");
        final JavaPairRDD<String, Boolean> blackListRdd = sc.parallelize(blackList).mapToPair(new PairFunction<String, String, Boolean>() {
            @Override
            public Tuple2<String, Boolean> call(String blackName) throws Exception {
                return new Tuple2<String, Boolean>(blackName, true);
            }
        });
        JavaReceiverInputDStream<String> adsClickLogStream = jssc.socketTextStream("localhost", 9999);
        /**
         * 2.对输入流数据进行转换操作，date userID变成(userID,date userID)
         *      以便于对每个batch Rdd与黑名单Rdd进行join
         */
        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
            }
        });
        //3.进行transform操作，将每个batch的Rdd与黑名单的Rdd进行join，filter，map操作，实时进行黑名单过滤
        userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                //通过左关联将黑名单数据过滤
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRdd = userAdsClickLogRDD.leftOuterJoin(blackListRdd);
                JavaRDD<String> validAdsClickLogRdd = joinedRdd.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> joinTuple) throws Exception {
                        if (joinTuple._2()._2().isPresent() && joinTuple._2()._2().get())
                            return false;
                        return true;
                    }
                }).map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2()._1();
                    }
                });
                return validAdsClickLogRdd;
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
