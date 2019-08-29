package com.hyr.spark.sharedvariables

import com.hyr.spark.utils.SparkUtils

/** *****************************************************************************
  * @date 2019-08-14 14:06
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object AccumulatorValue {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(AccumulatorValue.getClass)
    val accumulator = sparkContext.longAccumulator("My Accumulator")

    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sparkContext.makeRDD(list)

    listRDD.foreach(n => {
      accumulator.add(n)
    })

    println(accumulator.value)
  }

}
