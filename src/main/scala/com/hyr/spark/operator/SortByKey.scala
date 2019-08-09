package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-09 11:20
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:   对<key, value>结构的RDD进行升序或降序排列
  *                 1.comp：排序时的比较运算方式。
  *                 2.ascending：false降序；true升序。
  * *****************************************************************************/
object SortByKey {

  /**
    * ====================================================================
    * |            sortBy对RDD进行升序或降序排列  SortBy sorts RDD in ascending or descending order                    |
    * |            comp：排序时的比较运算方式。ascending：false降序；true升序。                                                        |
    * |            Comp: a comparative operation in sorting. Ascending:false descending; true ascending order.  |                                                                                                                                                                                                                              |
    * ====================================================================
    */
  def sortByKey(sparkContext: SparkContext): Unit = {
    val datas = List(60, 70, 80, 55, 45, 75)
    val rdd = sparkContext.makeRDD(datas)

    // 排序
    rdd.sortBy(num => {
      num
    }, ascending = true, 1).foreach(println)

    val names = sparkContext.makeRDD(List("cxk", "luce", "nike", "google", "apache", "druid"))
    val tuplesRDD = names.zip(rdd)

    tuplesRDD.sortBy(t => {
      t._2
    }).foreach(println)

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(SortByKey.getClass)

    sortByKey(sparkContext)
  }

}
