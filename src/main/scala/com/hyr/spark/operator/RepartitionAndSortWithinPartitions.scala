package com.hyr.spark.operator

import java.util.Random

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 16:00
  * @aut1, 2, 3, 7, 4, 5, 8hor: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 根据给定的Partitioner重新分区，并且每个分区内根据comp实现排序。
  * *****************************************************************************/
object RepartitionAndSortWithinPartitions {

  def repartitionAndSortWithinPartitions(sparkContext: SparkContext): Unit = {
    val datas = collection.mutable.ListBuffer[String]()
    val random = new Random(1)
    for (i <- 0 until 1000) {
      val str = "product%02d,url%03d".format(random.nextInt(10), random.nextInt(100))
      datas += str
    }

    val rdd = sparkContext.makeRDD(datas)
    val pairRDD = rdd.map(line => {
      val values = line.split(",")
      (values(0),values(1))
    })
    pairRDD.foreachPartition(t => {
      t.foreach(println)
    })

    pairRDD.repartition(10).foreachPartition(println) // 默认方式进行partition

    // 根据自定义的分区方式进行重分区
    pairRDD.repartitionAndSortWithinPartitions(new Partitioner {

      // 重分区数
      override def numPartitions: Int = {
        100
      }

      // 分区方式
      override def getPartition(key: Any): Int = {
        println("key:"+key)
        key.toString.substring(7).toInt
      }

    }).foreachPartition(t=>{
      t.foreach(println)
    })

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(RepartitionAndSortWithinPartitions.getClass)

    repartitionAndSortWithinPartitions(sparkContext)
  }

}
