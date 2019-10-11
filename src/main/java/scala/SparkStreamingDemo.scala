package scala

import org.apache.spark._;
import org.apache.spark.streaming._;
import org.apache.spark.streaming.StreamingContext._;

object SparkStreamingDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream("xdata-Hadoop3", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val count = pairs.reduceByKey(_ + _)
    count.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
