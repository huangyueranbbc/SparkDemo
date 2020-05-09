package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 08:39
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对多个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
  ******************************************************************************/
object Cogroup {

  def cogroup(sparkContext: SparkContext): Unit = {
    val datas1 = List((1, "苹果"), (2, "梨"), (3, "香蕉"), (4, "石榴"))

    val datas2 = List((1, 7), (2, 3), (3, 8), (4, 3))

    val datas3 = List((1, "7"), (2, "3"), (3, "8"), (4, "3"), (4, "4"), (4, "5"), (4, "6"))

    val rdd1 = sparkContext.parallelize(datas1)
    val rdd2 = sparkContext.parallelize(datas2)
    val rdd3 = sparkContext.parallelize(datas3)

    val rdd = rdd1.cogroup(rdd2,rdd3)
    rdd.foreach(tuple => {
      println("key:" + tuple._1 + "\tvalue:" + tuple._2)
    })

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(Cogroup.getClass)
    cogroup(sparkContext)
  }

}
