package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 09:16
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对元素进行过滤，对每个元素应用f函数，返回值true的元素在RDD中保留，返回为false的将过滤掉
  ******************************************************************************/
object Filter {

  def filter(sparkContext: SparkContext): Unit = {
    val datas = List(1, 2, 3, 7, 4, 5, 8)
    val rdd = sparkContext.parallelize(datas)
    val filterRDD = rdd.filter(x => x >= 4) // 过滤小于4的数
    filterRDD.foreach(t => {
      println(t)
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Filter.getClass)

    filter(sparkContext)
  }

}
