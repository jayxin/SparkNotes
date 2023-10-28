package net.homework;

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.nio.file.Paths

object App {
  def main(args : Array[String]) : Unit = {
    // Get the path of input and output
    val cwd = Paths.get("").toAbsolutePath.toString
    val inputPath = cwd + "/input/"
    val outputPath = "file://" + cwd + "/output"

    // Get spark context
    val conf = new SparkConf().setMaster("local").setAppName("CalScore");
    val sc = new SparkContext(conf)

    val textData = sc.textFile("file://" + inputPath + "score0.txt")
    textData.cache()

    var fields : Array[String] = textData.first().split(" ")
    var msg = "course\taverage\tmin\tmax"

    // Get the information of student
    val info = textData.map(
        line => {
          if (line.startsWith("Id")) {
            List()
          } else {
            line.split(" ").toList
          }
        }
    ).filter(_.isEmpty == false)
    info.cache()

    // Calculate score for the whole class
    cal(info, msg, fields)

    // Calculate score for the males
    val maleInfo = info.filter(_.contains("male"))
    maleInfo.cache()
    cal(maleInfo, s"${msg}(males)", fields)

    // Calculate score for the females
    val femaleInfo = info.filter(_.contains("female"))
    femaleInfo.cache()
    cal(femaleInfo, s"${msg}(females)", fields)
  }

  def cal(info : RDD[List[String]], msg : String, fields : Array[String]) {
    println(msg)

    val numOfPerson = info.count
    val len = fields.length

    for (index <- 2 until len) {
      val scores = info.map(_(index).toDouble)
      scores.cache

      val max = f"${scores.max}%.2f"
      val min = f"${scores.min}%.2f"
      val sum = scores.reduce((x, y) => x + y)
      val avg = f"${sum / numOfPerson}%.2f"

      println(s"${fields(index)}: ${avg}\t${min}\t${max}")
    }
  }
}
