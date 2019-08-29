package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  *
  * @date 2019-08-07 17:04
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 两个RDD进行笛卡尔积合并--The two RDD are Cartesian product merging
  ******************************************************************************/
object Cartesian {


  def cartesian(sparkContext: SparkContext): Unit = {
    val names = List("张三", "李四", "王五")
    val scores = List(60, 70, 90)

    val namesRDD = sparkContext.parallelize(names)
    val scoresRDD = sparkContext.parallelize(scores)

    val cartesianRDD = namesRDD.cartesian(scoresRDD)

    cartesianRDD.foreach(tuple => {
      println("key:"+tuple._1+"\tvalue:"+tuple._2)
    })

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(Cartesian.getClass)

    cartesian(sparkContext)
  }

}
