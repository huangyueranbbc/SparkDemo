package com.hyr.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-09 10:44
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
  * *****************************************************************************/
object SampleAndTake {

  def sampleAndTake(sparkContext: SparkContext): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf setAppName "SampleAndTake" setMaster "local"
    val sparkContext = new SparkContext(sparkConf)

    sampleAndTake(sparkContext)
  }

}
