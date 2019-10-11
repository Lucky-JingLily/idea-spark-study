package scala

import org.apache.spark.{SparkConf, SparkContext}

object CartesianDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
//    val rdd1 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-1.txt", 4)
//    val rdd2 = sc.textFile("D://Office//IdeaProjects//scalaDemo//cogroup-2.txt", 4)
    val rdd1 = sc.parallelize(Array("Tom", "Tomas", "Tomasle", "tomson"))
    val rdd2 = sc.parallelize(Array("123", "234", "123e34", "rwerf"))

    val rdd3 = rdd1.cartesian(rdd2)
    rdd3.collect().foreach(t => println(t))
  }
}
