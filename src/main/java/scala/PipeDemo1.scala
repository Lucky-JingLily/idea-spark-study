package scala

import org.apache.spark.{SparkConf, SparkContext}

object PipeDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
//    val rdd1 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-1.txt", 4)
//    val rdd2 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-2.txt", 4)
    sc.parallelize(Array("aa", "bb", "cc")).pipe("echo").collect().foreach(println)
  }
}
