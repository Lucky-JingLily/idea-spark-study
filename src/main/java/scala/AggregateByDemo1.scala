package scala

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByDemo1 {
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
//    val rdd3 = rdd2.aggregateByKey(0)((a:Int,b:String)=>1, (c:Int, d:Int) => c+d)
//    for ((k, v) <- rdd3.collect()) {
//      println(k + ", " + v)
//    }
//    val rdd2 = rdd1.flatMap(_.split(" "));
//    val rdd3 = rdd2.map((_, 1))
    val rdd4 = rdd2.sortByKey()
    for ((k, v) <- rdd4.collect()) {
      println(k + ", " + v)
    }
  }
}
