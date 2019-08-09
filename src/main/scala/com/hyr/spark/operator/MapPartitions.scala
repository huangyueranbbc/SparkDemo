package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/** *****************************************************************************
  * @date 2019-08-08 13:55
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。所以func的类型是Iterator
  * *****************************************************************************/
object MapPartitions {

  def mapPartitions(sparkContext: SparkContext): Unit = {
    val names = List("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4")

    val namesRDD = sparkContext.parallelize(names, 3) // 3个分片

    val mapPartitionsRDD = namesRDD.mapPartitions(element => {
      var list = new ArrayBuffer[String]()
      while (element.hasNext) {
        val name = element.next()
        println(name)
        list.+=(name)
      }
      list.iterator
    })

    mapPartitionsRDD.collect().foreach(println)


  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(MapPartitions.getClass)

    mapPartitions(sparkContext)
  }

}
