/**
  * Created by aseara on 2017/6/1.
  */

package com.github.aseara.spark

import org.apache.spark._

/**
  * Created by qiujingde on 2017/5/31.
  * spark 集群测试1：随机点计算圆周率
  */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.8.192:7077")
    val spark = new SparkContext(conf)
    spark.addJar("E:\\study\\spark\\scala-pi\\target\\scala-2.11\\scala-pi_2.11-1.0.jar")
    val slices = 200
    val n = math.min(100000L * slices, Int.MaxValue).toInt
    val count = spark.parallelize(1 to n, slices).map { _ =>
      val x = math.random
      val y = math.random
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_+_)
    System.out.println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
