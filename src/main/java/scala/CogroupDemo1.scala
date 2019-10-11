package scala

import org.apache.spark.{SparkConf, SparkContext}

object CogroupDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-1.txt", 4)
    val rdd2 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-2.txt", 4)

    val rdd3 = rdd1.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1))
    })
    val rdd4 = rdd2.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1))
    })

    rdd3.cogroup(rdd4).collect().foreach(t => {
      println(t._1 + ": -----")
      for (e <- t._2._1) {
        print(e + " ")
      }
      println()
      for (e <- t._2._2) {
        print(e + " ")
      }
      println()
    })

    rdd4.cartesian(rdd4).collect().foreach(t => {
      println(t._1 + ": -----")
      for (e <- t._2._1) {
        print(e + " ")
      }
      println()
      for (e <- t._2._2) {
        print(e + " ")
      }
      println()
    })
  }
}
