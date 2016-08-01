package cn.com.dwsoft.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2016/1/5.
 * 使用JDBC数据源读取数据并处理保存
 */

/**
 * DF一些操作使用
 * DataFrame people = sqlContext.read().parquet("...");
 * DataFrame department = sqlContext.read().parquet("...");
 * <p/>
 * people.filter("age".gt(30))
 * .join(department, people.col("deptId").equalTo(department("id")))
 * .groupBy(department.col("name"), "gender")
 * .agg(avg(people.col("salary")), max(people.col("age")));
 */
public class JDBCDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //JDBC连接信息
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://study1:3306/test?user=root&password=root");
        options.put("dbtable", "EMP");
        DataFrame emp = sqlContext.read().format("jdbc").options(options).load();
        options.put("dbtable", "DEPT");
        DataFrame dept = sqlContext.read().format("jdbc").options(options).load();
        DataFrame joinDF = emp.filter(emp.col("SAL").gt(1200)).join(dept, "DEPTNO")
                .orderBy(emp.col("DEPTNO"), emp.col("SAL"));
        joinDF.show();
    }
}
