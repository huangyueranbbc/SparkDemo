package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** *****************************************************************************
  * @date 2019-08-08 14:39
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 与mapPartitions类似，但输入会多提供一个整数表示分区的编号，所以func的类型是(Int, Iterator)
  * *****************************************************************************/
object MapPartitionsWithIndex {

  def mapPartitions(sparkContext: SparkContext): Unit = {
    val names = List("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4")

    val namesRDD = sparkContext.parallelize(names, 5) // 5个分片

    // 附带partition信息
    val mapPartitionsRDD = namesRDD.mapPartitionsWithIndex((partition,element) => {
      var list = new ArrayBuffer[String]()
      while (element.hasNext) {
        val name = element.next()
        println(partition+"\t"+name)
        list.+=(partition+"\t"+name)
      }
      list.iterator
    })

    mapPartitionsRDD.collect().foreach(println)


  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(MapPartitionsWithIndex.getClass)

    mapPartitions(sparkContext)
  }

}
