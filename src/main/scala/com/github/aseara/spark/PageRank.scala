package com.github.aseara.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by aseara on 2017/6/23.
  */
object PageRank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.8.192:7077")
    val sc = new SparkContext(conf)
    sc.addJar("E:\\study\\spark\\scala-pi\\target\\scala-2.11\\scala-pi_2.11-1.0.jar")

    val links = sc.objectFile[(String, Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()

    var ranks = links.mapValues(v => 1.0)

    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (_, (links, rank)) =>
          links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey(_+_).mapValues(0.15+0.85*_)
    }

    ranks.saveAsTextFile("ranks")

    sc.stop()
  }
}
