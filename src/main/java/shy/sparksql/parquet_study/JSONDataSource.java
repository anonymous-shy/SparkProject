package shy.sparksql.parquet_study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2016/1/4.
 */
public class JSONDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JSONDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //通过json创建DataFrame
        DataFrame json = sqlContext.read().json("hdfs://10.13.244.41:9000/spark/students.json");
        json.registerTempTable("stud");
        DataFrame sql = sqlContext.sql("select * from stud where score >= 80");
        List<String> studInfoJsons = new ArrayList<String>();
        studInfoJsons.add("{\\\"name\\\":\\\"Leo\\\", \\\"age\\\":18}");
        studInfoJsons.add("{\\\"name\\\":\\\"Marry\\\", \\\"age\\\":17}");
        studInfoJsons.add("{\\\"name\\\":\\\"Jack\\\", \\\"age\\\":19}");
        //通过list创建RDD
        JavaRDD<String> parallelize = sc.parallelize(studInfoJsons);
        DataFrame json1 = sqlContext.read().json(parallelize);
        json1.registerTempTable("studInfo");
        DataFrame sql1 = sqlContext.sql("select * from studInfo");
        JavaPairRDD<String, Tuple2<Integer, Integer>> join = sql.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), (int) row.getLong(1));
            }
        }).join(sql1.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), (int) row.getLong(1));
            }
        }));
        JavaRDD<Row> map = join.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1(), tuple._2()._1(), tuple._2()._2());
            }
        });
        List<StructField> structFieldList = new ArrayList<StructField>();
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("score", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFieldList);
        DataFrame dataFrame = sqlContext.createDataFrame(map, structType);
        dataFrame.write().format("json").save("hdfs://10.13.244.41:9000/spark/stud-out");
    }
}
