package com.hyr.spark.sharedvariables

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.SparkContext

/** *****************************************************************************
  * @date 2019-08-19 14:56
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object BroadCastValue {

  def broadCastValue(sparkContext: SparkContext): Unit = {
    val num = 3
    val broadcast = sparkContext.broadcast(num)
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sparkContext.makeRDD(list)
    val resultRDD = listRDD.map(n => {
      n * broadcast.value
    })

    resultRDD.foreach(println)

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(BroadCastValue.getClass)
    broadCastValue(sparkContext)
  }


}
