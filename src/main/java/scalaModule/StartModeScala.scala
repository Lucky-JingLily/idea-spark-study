package scalaModule

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object StartModeScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setMaster("local[4, 2]");
    conf.setAppName("wordcount");

    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(1 to 20);
//    val rdd2 = rdd1.map(e => {
//      var tname = Thread.currentThread().getName;
//      println("TName: " + tname)
//      e * 2
//    })
    val rdd2 = rdd1.map(e => {
      var tname = Thread.currentThread().getName;
      println("TName: " + tname + ", e = " + e)
      if (e == 2) {
        val f = new File("src/main/resources/list")
        if (f.exists()) {
          f.delete()
          "xxx".toInt
        } else {
          e
        }
      } else {
        e
      }
    })
//    val rdd3 = rdd1.repartition(5)
//    rdd3.foreach(e => {
//      var tname = Thread.currentThread().getName;
//      println("TName: " + tname + ", e = " + e)
//      e * 2
//    })
//    rdd3.collect()
    println(rdd2.reduce(_ + _))
  }
}
