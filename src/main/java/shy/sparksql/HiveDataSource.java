package shy.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by root on 2016/1/5.
 * 使用HiveContext的sql()/hql()的方法,可以执行Hive中的HiveQl语句操作Hive中的表
 */
public class HiveDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //通过sc.sc()拿到SparkContext
        HiveContext hiveContext = new HiveContext(sc.sc());
        //DROP TABLE
        hiveContext.sql("DROP TABLE IF EXISTS EMP");
        hiveContext.sql("DROP TABLE IF EXISTS DEPT");
        //CREATE TABLE
        hiveContext.sql("CREATE TABLE IF NOT EXISTS EMP " +
                        "(EMPNO INT,ENAME STRING,JOB STRING,MGR INT," +
                        "HIREDATE STRING,SAL INT,COMM INT,DEPTNO INT)" +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
        );
        hiveContext.sql("CREATE TABLE IF NOT EXISTS DEPT " +
                        "(DEPTNO INT,DNAME STRING,LOC STRING)" +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' "
        );
        //LOAD DATA
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/victor/emp' INTO TABLE EMP ");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/home/victor/dept' INTO TABLE DEPT ");
        DataFrame joinDataFrame = hiveContext.sql("SELECT e.EMPNO,e.ENAME,e.SAL,e.DEPTNO,d.DNAME " +
                "FROM EMP e inner join DEPT d ON e.DEPTNO = d.DEPTNO " +
                "WHERE e.SAL >= 1200 ");
        joinDataFrame.saveAsTable("SAL_GT1200");
        joinDataFrame.show();
    }
}
