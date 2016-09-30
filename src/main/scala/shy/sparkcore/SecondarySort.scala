package shy.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2016/4/1.
 * Spark实现二次排序
 */
object SecondarySort extends App {

    val sparkConf = new SparkConf().setAppName(" Secondary Sort ")
    sparkConf.set("mapreduce.framework.name", "yarn");
    sparkConf.set("spark.rdd.compress", "true");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.storage.memoryFraction", "0.5");
    sparkConf.set("spark.akka.frameSize", "100");
    sparkConf.set("spark.default.parallelism", "1");
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("hdfs://namenode:9000/test/secsortdata")
    val rdd = file.map(line => line.split("\t")).
      map(x => (x(0), x(1))).groupByKey().
      sortByKey(true).map(x => (x._1, x._2.toList.sortWith(_ > _)))
    val rdd2 = rdd.flatMap {
      x =>
        val len = x._2.length
        val array = new Array[(String, String)](len)
        for (i <- 0 until len) {
          array(i) = (x._1, x._2(i))
        }
        array
    }
    sc.stop()
}
