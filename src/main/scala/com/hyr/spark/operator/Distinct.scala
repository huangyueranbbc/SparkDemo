package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 09:12
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对原RDD进行去重操作，返回RDD中没有重复的成员---Performs a reset operation on the original RDD and returns no duplicate members in the RDD
  * *****************************************************************************/
object Distinct {

  def distinct(sparkContext: SparkContext): Unit = {
    val datas = List("张三", "李四", "tom", "张三")
    val rdd = sparkContext.parallelize(datas)
    val distinctRDD = rdd.distinct()
    distinctRDD.foreach(tuple => {
      println(tuple)
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Distinct.getClass)
    distinct(sparkContext)
  }

}
