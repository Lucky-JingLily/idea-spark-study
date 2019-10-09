package scalaModule

import org.apache.spark.{SparkConf, SparkContext}
import java.text.SimpleDateFormat
import java.util.Date

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//    conf.setMaster("local[4]")
    val dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    var startTime = "11/11/11 11:11:11"
    var finishTime = dateFormat.format(new Date())
    var cutFile = ""
    var saveFile = "/tmp/appLogCut"
    if (args.length >= 4) {
      startTime = args(0)
      finishTime = args(1)
      cutFile = args(2)
      saveFile = args(3)
    }

    conf.setAppName("appLogCutFile")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(cutFile)
    val endTime = dateFormat.parse(finishTime).getTime
    val fromTime = dateFormat.parse(startTime).getTime
    var startFlag = 0
    val rdd2 = rdd1.map(line => {
      try {
        val items = line.split(" ")
        val dateTime = items(0) + " " + items(1)
        val formatTime: Long = new SimpleDateFormat("yy/MM/dd HH:mm:ss").parse(dateTime).getTime
        if (formatTime >= fromTime || startFlag == 1) {
          startFlag = 1
          line
        } else if (formatTime <= endTime && startFlag == 1) {
          startFlag = 0
        } else {
          ""
        }
      } catch {
        case e: Exception => {
          line
        }
      }
    })
    rdd2.filter(line => line != "").repartition(1).saveAsTextFile(saveFile)
  }
}