package java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.Properties;
import java.util.function.Consumer;

public class SQLJDBCJava {
    public static void main(String[] args) throws AnalysisException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SQLJava");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().appName("SQLJava").config("spark.master", "local").getOrCreate();
        Dataset<Row> df = session.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mybatis")
                .option("dbtable", "items")
                .option("user", "root")
                .option("password", "mysql")
                .option("driver", "com.mysql.jdbc.Driver")
                .load();
        df.show();

        Dataset<Row> df2 = df.select(new Column("itemname"));
        df2.show();

        df2 = df2.where("itemname like 'item%'");
        df2.show();

        Properties prop = new Properties();
        prop.put("user", "root");
        prop.put("password", "mysql");
        prop.put("driver", "com.mysql.jdbc.Driver");
        df2.write().jdbc("jdbc:mysql://localhost:3306/mybatis", "subitems", prop);
    }
}
