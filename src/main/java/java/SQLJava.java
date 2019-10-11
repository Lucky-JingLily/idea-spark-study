package java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.function.Consumer;

public class SQLJava {
    public static void main(String[] args) throws AnalysisException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SQLJava");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder().appName("SQLJava").config("spark.master", "local").getOrCreate();
        Dataset<Row> df = session.read().json("SparkDemo1/src/main/resources/json.dat");
        df.show();

        df.createTempView("customers");

        session.sql("select count(1) from customers").show();

        df.where("id < 2").show();

        JavaRDD<Row> rdd = df.toJavaRDD();
        rdd.collect().forEach(
                new Consumer<Row>() {
                    public void accept(Row row) {
                        String age = row.getString(0);
                        String id = row.getString(1);
                        String name = row.getString(2);
                        System.out.println(age + ", " + id + ", " + name);
                    }
                }
        );

        df.write().mode(SaveMode.Append).json("SparkDemo1/src/main/resources/output.dat");
    }
}
