package scala

import org.apache.spark.{SparkConf, SparkContext}

object MapDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D://helloworld.txt", 4)
    val rdd2 = rdd1.flatMap(_.split(" "))
//    val rdd3 = rdd2.map(word => {println("start"); val t = (word, 1); println(t + "end"); t});
//    val rdd3 = rdd2.mapPartitions(it => {
//      import scala.collection.mutable.ArrayBuffer;
//      val buf = ArrayBuffer[String]()
//      val tname = Thread.currentThread().getId
//      println(tname + " mapPartitions start")
//      for (e <- it) {
//        buf.+=("_" + e)
//      }
//      buf.iterator
//    })
    val rdd3 = rdd2.mapPartitionsWithIndex((index, it) => {
      import scala.collection.mutable.ArrayBuffer;
      val buf = ArrayBuffer[String]()
      val tname = Thread.currentThread().getId
      println(tname + " : " + index + " : mapPartitions start")
      for (e <- it) {
        buf.+=("_" + e)
      }
      buf.iterator
    })
    val rdd5 = rdd3.map(word => {
      val tname = Thread.currentThread().getId
      println(tname + " map " + word)
      (word, 1)
    })
    val rdd4 = rdd5.reduceByKey(_ + _)
    val r = rdd4.collect()
    //    for(i <- 0 until(r.length)) {
    //      println(r(i))
    //    }
    r.foreach(println(_))
//    println(sc.defaultParallelism)
  }
}
