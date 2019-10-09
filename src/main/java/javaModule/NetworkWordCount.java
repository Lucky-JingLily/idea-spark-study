package javaModule;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NetworkWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("NetworkWordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(5));

        jsc.checkpoint("hdfs://ns1/tmp/checkpoint");

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("xdata-Hadoop3", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] strs = s.split(" ");
                for (String str :
                        strs) {
                    list.add(str);
                }
                return list.iterator();
            }
        });

        JavaPairDStream<String, Integer> paris = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String,Integer> jps = paris.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> v1, Optional<Integer> v2) throws Exception {
                Integer newCount = v2.isPresent() ? v2.get() : 0  ;

                System.out.println("old value : " + newCount);
                for(Integer i : v1){
                    System.out.println("new value : " + i);
                    newCount = newCount +  i;
                }
                return Optional.of(newCount);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = jps.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();

        jsc.start();
        jsc.awaitTermination();

    }
}
