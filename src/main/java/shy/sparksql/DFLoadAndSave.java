package shy.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 2015/12/31.
 * 通过load直接生成DataFrame
 */
public class DFLoadAndSave {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DFLoadAndSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().load("hdfs://10.13.244.41:9000/spark/users.parquet");
//        dataFrame.printSchema();
/*
root
|-- name: string (nullable = false)
|-- favorite_color: string (nullable = true)
|-- favorite_numbers: array (nullable = false)
|    |-- element: integer (containsNull = false)
*/
//        dataFrame.show();
/*
+------+--------------+--------------------+
|  name|favorite_color|    favorite_numbers|
+------+--------------+--------------------+
|Alyssa|          null|ArrayBuffer(3, 9,...|
|   Ben|           red|       ArrayBuffer()|
+------+--------------+--------------------+
*/
        dataFrame.select("name", "favorite_color").write()
                .save("hdfs://10.13.244.41:9000/spark/namesAndFavColors.parquet");
    }
}
