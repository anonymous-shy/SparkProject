package shy.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2016/1/6.
 */
public class JDBC2Save2HDFS {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //JDBC连接信息
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://study1:3306/test?user=root&password=root");
        options.put("dbtable", "EMP");
        DataFrame emp = sqlContext.read().format("jdbc").options(options).load();
        emp.write().save("hdfs://10.13.244.41:9000/spark/emp");
        options.put("dbtable", "DEPT");
        DataFrame dept = sqlContext.read().format("jdbc").options(options).load();
        dept.write().save("hdfs://10.13.244.41:9000/spark/dept");
    }
}
