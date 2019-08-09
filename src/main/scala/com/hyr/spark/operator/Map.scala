package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 14:42
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: map
  ******************************************************************************/
object Map {

  def map(sparkContext: SparkContext): Unit = {
    val data = List("hello,hyr", "hello,zhoujielun")
    val rdd = sparkContext.makeRDD(data)
    rdd.map(str => {
      str.split(",")
    }).foreach(t => {
      println(t.getClass.getName) // map是一个String数组对象,flagmap会返回String字符串
      t.foreach(s=>{
        println(s)
      })
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Map.getClass)

    map(sparkContext)

  }

}
