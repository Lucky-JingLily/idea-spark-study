package scala

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PersistScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("wordcount")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(1 to 20)
    val rdd2 = rdd1.map(e => {
      println(e)
      e * 2
    })
//    rdd2.cache()
    rdd2.persist(StorageLevel.DISK_ONLY)
    rdd2.getCheckpointFile
    println(rdd2.reduce(_ + _))
    println(rdd2.reduce(_ + _))
    rdd2.unpersist()
    println(rdd2.reduce(_ + _))

    var str:String = java.net.InetAddress.getLocalHost.getHostAddress;
    str = str + ":" + Thread.currentThread().getName + "\r\n"

    val sock = new java.net.Socket("xdata-Hadoop3", 8888)
    val out = sock.getOutputStream
    out.write(str.getBytes())
    out.flush()
    out.close()
    sock.close()
  }
}
