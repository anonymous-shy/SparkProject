package shy.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2015/12/28.
 * 通过动态转化的方式将RDD转化为DataFrame，较常用，直接通过字段类型和个数映射
 */
public class RDD2DataFrame2 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> textFile = sc.textFile("hdfs://10.13.244.41:9000/spark/star");
        /**
         *第一步:将textFile转化为map的RDD<Row>的格式，里面为文件中的行数据
         */
        JavaRDD<Row> rowJavaRDD = textFile.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                //在create数据的时候一定要注意数据格式
                return RowFactory.create(split[0], split[1], Integer.valueOf(split[2]));
            }
        });
        /**
         *第二步:动态构造元数据
         */
        List<StructField> structFieldList = new ArrayList<StructField>();
        structFieldList.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFieldList);
        /**
         * 第三步:使用动态构造的元数据,将RDD转化为DataFrame
         */
        DataFrame dataFrame = sqlContext.createDataFrame(rowJavaRDD, structType);
        dataFrame.registerTempTable("stars");
        DataFrame sqlDF = sqlContext.sql("select * from stars where age > 25");
        sqlDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });
    }
}
