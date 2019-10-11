package scala

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(args(0))
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_, 1));
    val rdd4 = rdd3.reduceByKey(_ + _)
    val r = rdd4.collect()
//    for(i <- 0 until(r.length)) {
//      println(r(i))
//    }
    r.foreach(println(_))
  }
}
