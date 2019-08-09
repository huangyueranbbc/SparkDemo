package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  *
  * @date 2019-08-09 10:44
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
  ******************************************************************************/
object SampleAndTake {

  def sampleAndTake(sparkContext: SparkContext): Unit = {
    val datas = List(1, 2, 3, 7, 4, 5, 8)
    val rdd = sparkContext.makeRDD(datas)
    // 是否重复采样/采样阈值[0,1]/随机数发生种子
    val sampleRDD = rdd.sample(withReplacement = false, 0.75, System.currentTimeMillis())
    sampleRDD.foreach(println)
    // 元素可以多次抽样(在抽样时替换)/返回样本大小/随机数种子
    sampleRDD.takeSample(withReplacement = true, 3, System.currentTimeMillis).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(SampleAndTake.getClass)

    sampleAndTake(sparkContext)
  }

}
