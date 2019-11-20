package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-07 17:24
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。
  *               如果shuffle设置为true，则会进行shuffle。可以在Filter后进行Coalesce重分区来减少数据倾斜。
  ******************************************************************************/
object Coalesce {

  def coalesce(sparkContext: SparkContext): Unit = {
    val datas = List("hi", "hello", "how", "are", "you")
    val rdd1 = sparkContext.parallelize(datas, 4)
    println("rdd partitions num:" + rdd1.getNumPartitions)
    val rdd2 = rdd1.coalesce(2, shuffle = false)
    println("rdd partitions num:" + rdd2.getNumPartitions)
    val rdd3 = rdd1.repartition(100)
    println("rdd partitions num:" + rdd3.getNumPartitions)

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Cartesian.getClass)
    coalesce(sparkContext)
  }

}
