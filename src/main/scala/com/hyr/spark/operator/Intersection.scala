package com.hyr.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 10:32
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 返回两个RDD的交集--Returns the intersection of two RDD
  ******************************************************************************/
object Intersection {

  def intersection(sparkContext: SparkContext): Unit = {
    val data1 = List("张三", "李四", "tom")
    val data2 = List("tom", "gim")
    val rdd = sparkContext.makeRDD(data1).intersection(sparkContext.makeRDD(data2))
    rdd.foreach(t => {
      println(t)
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf setAppName "Intersection" setMaster "local"
    val sparkContext = new SparkContext(sparkConf)

    intersection(sparkContext)
  }


}
