package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 15:02
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对RDD成员使用func进行reduce操作，func接受两个参数，合并之后只返回一个值。reduce操作的返回结果只有一个值。需要注意的是，func会并发执行
  ******************************************************************************/
object Reduce {

  def reduce(sparkContext: SparkContext): Unit = {
    val data = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sparkContext.makeRDD(data)

    val sum = rdd.reduce((result: Int, value: Int) => {
      println("result:" + result + "\tvalue:" + value)
      result + value
    })
    println("sum:"+sum)
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Reduce.getClass)

    reduce(sparkContext)
  }

}
