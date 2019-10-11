package scala

import org.apache.spark.{SparkConf, SparkContext}

object GroupByDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D://helloworld.txt", 4)
    val rdd2 = rdd1.map(line => {
      val key = line.split(" ")(3)
      (key, line)
    })
    val rdd3 = rdd2.groupByKey()
    rdd3.collect().foreach(t => {
      val key = t._1;
      println(key + "=====================")
      for (e <- t._2) {
        println(e)
      }
      }
    )
  }
}
