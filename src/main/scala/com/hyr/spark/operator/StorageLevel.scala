package com.hyr.spark.operator

import java.io.File

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-09 12:00
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  ******************************************************************************/
object StorageLevel {

  def storageLevel(sparkContext: SparkContext): Unit = {

    val text = sparkContext.textFile(Constant.LOCAL_FILE_PREX+"/data/resources/wc_data")
    text.persist(org.apache.spark.storage.StorageLevel.NONE)

    val starttime = System.currentTimeMillis
    val count = text.count
    System.out.println("count:" + count)
    val endtime = System.currentTimeMillis
    System.out.println("costtime:" + (endtime - starttime))

    val starttime2 = System.currentTimeMillis
    val count2 = text.count
    System.out.println("count2:" + count2)
    val endtime2 = System.currentTimeMillis
    System.out.println("costtime2:" + (endtime2 - starttime2))

    val starttime3 = System.currentTimeMillis
    val count3 = text.count
    System.out.println("count3:" + count3)
    val endtime3 = System.currentTimeMillis
    System.out.println("costtime3:" + (endtime3 - starttime3))
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(StorageLevel.getClass)

    // 当RDD会被复用的时候通常我们需要使用持久化策略
    // 1.持久化策略默认的是MEMORY_ONLY
    // 2如果内存有些吃紧，可以选择MEMORY_ONLY_SER
    // 3.当我们的数据想要做一定的容错可以使用_2
    // 4.如果我们的中间结果RDD计算代价比较大，那我们可以选择MEMORY_AND_DISK
    // memory_only就算存不下就不存了
    // MEMORY_AND_DISK如果内存存不下会存到本地磁盘空间

    storageLevel(sparkContext)
  }

}
