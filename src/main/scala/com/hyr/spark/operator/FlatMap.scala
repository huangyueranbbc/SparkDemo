package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 09:27
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 与map类似，但每个输入的RDD成员可以产生0或多个输出成员 扁平化输出
  * *****************************************************************************/
object FlatMap {

  def flatMap(sparkContext: SparkContext): Unit = {
    val datas = List("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript")

    val rdd = sparkContext.parallelize(datas)

    rdd.flatMap(str => {
      str.split(",")
    }).foreach(t => {
      println(t)
    })

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(FlatMap.getClass)

    flatMap(sparkContext)
  }

}
