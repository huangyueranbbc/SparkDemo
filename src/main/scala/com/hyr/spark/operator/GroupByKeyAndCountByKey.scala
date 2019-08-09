package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 09:57
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对<key, value>结构的RDD进行类似RMDB的group by聚合操作，具有相同key的RDD成员的value会被聚合在一起，返回的RDD的结构是(key, Iterator<value>)
  ******************************************************************************/
object GroupByKeyAndCountByKey {

  def countByKey(rdd: RDD[(Int, String)]): Unit = {
    rdd.countByKey().foreach(t => {
      println(t)
    })
  }

  def groupBy(sparkContext: SparkContext): Unit = {
    // 根据奇数偶数分组
    val datas = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val rdd = sparkContext.parallelize(datas)
    val rdd1 = rdd.groupBy(x => {
      x % 2 match {
        case 0 => "偶数"
        case _ => "基数"
      }
    })
    rdd1.foreach(t => {
      println(t)
    })

    // 根据key分组
    val datas1 = List((1, "a"), (1, "b"), (1, "c"), (2, "d"), (2, "e"))
    sparkContext.parallelize(datas1).groupBy(
      x => {
        x._1
      }
    ).foreach(t => {
      println(t)
    })

    // 根据value分组
    sparkContext.parallelize(datas1).groupBy(
      x => {
        x._2
      }
    ).foreach(t => {
      println(t)
    })

    // countByKey
    countByKey(sparkContext.parallelize(datas1))

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(GroupByKeyAndCountByKey.getClass)

    groupBy(sparkContext)
  }


}
