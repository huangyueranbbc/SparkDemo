package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.SparkContext

/** *****************************************************************************
  * @date 2019-08-09 15:47
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  ******************************************************************************/
object Union {

  def union(sparkContext: SparkContext): Unit = {
    val data1 = List("张三", "李四")
    val data2 = List("tom", "gim")

    val rdd1 = sparkContext.makeRDD(data1)
    val rdd2 = sparkContext.makeRDD(data2)

    rdd1.union(rdd2).foreach(t=>{
      println(t)
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Union.getClass)
    union(sparkContext)
  }

}
