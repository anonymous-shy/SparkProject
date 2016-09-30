package shy.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 2015/12/22.
 */
@SuppressWarnings({"unused", "unchecked"})
public class JavaTransOperation {
    public static void main(String[] args) {
//        mapOperation();
//        filterOperation();
        flatMapOperation();
//        groupByKeyOperation();
//        reduceByKeyOperation();
//        joinOperation();
    }

    /**
     * map operation
     */
    public static void mapOperation() {
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);
        SparkConf conf = new SparkConf().setAppName("mapOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> javaRDD = sc.parallelize(nums);
        JavaRDD<Integer> mapRDD = javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num * 2;
            }
        });
        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer num2) throws Exception {
                System.out.println(num2);
            }
        });
        sc.close();
    }

    /**
     * filter operation
     */
    public static void filterOperation() {
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        SparkConf conf = new SparkConf().setAppName("mapOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> parallelize = sc.parallelize(nums);
        JavaRDD<Integer> filter = parallelize.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });
        filter.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * flatMap operation
     */
    public static void flatMapOperation() {
        SparkConf conf = new SparkConf().setAppName("flatMapOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("hdfs://10.13.244.41:9000/spark/wc.input");
        JavaRDD<String> flatMap = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t"));
            }
        });
        flatMap.saveAsTextFile("hdfs://10.13.244.41:9000/spark/wc.output");
        flatMap.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /**
     * groupByKey operation
     */
    public static void groupByKeyOperation() {
        SparkConf conf = new SparkConf().setAppName("groupByKeyOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("Hadoop", 89),
                new Tuple2<String, Integer>("Spark", 95),
                new Tuple2<String, Integer>("Storm", 90),
                new Tuple2<String, Integer>("Hadoop", 60),
                new Tuple2<String, Integer>("Storm", 30),
                new Tuple2<String, Integer>("Spark", 50)
        );
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairs.groupByKey();
        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                System.out.println("Subject: " + tuple._1());
                for (Integer integer : tuple._2()) {
                    System.out.println(integer);
                }
                System.out.println("----------");
            }
        });
    }

    public static void reduceByKeyOperation() {
        SparkConf conf = new SparkConf().setAppName("groupByKeyOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("Hadoop", 89),
                new Tuple2<String, Integer>("Spark", 95),
                new Tuple2<String, Integer>("Storm", 90),
                new Tuple2<String, Integer>("Hadoop", 60),
                new Tuple2<String, Integer>("Storm", 30),
                new Tuple2<String, Integer>("Spark", 50)
        );
        JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> reduceByKey = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        reduceByKey.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t2) throws Exception {
                System.out.println(t2._1() + " : " + t2._2());
            }
        });
    }

    public static void joinOperation() {
        SparkConf conf = new SparkConf().setAppName("joinOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "Shy"),
                new Tuple2<Integer, String>(2, "Emma"),
                new Tuple2<Integer, String>(3, "Taylor")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 90),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 70)
        );

        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = studentRDD.join(scoreRDD);
        join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> joined) throws Exception {
                System.out.println("ID: " + joined._1());
                System.out.println("NAME: " + joined._2()._1());
                System.out.println("SCORE: " + joined._2()._2());
                System.out.println("----------");
            }
        });
    }
}
