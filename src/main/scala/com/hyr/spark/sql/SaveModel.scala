package com.hyr.spark.sql

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/** *****************************************************************************
  * @date 2019-08-29 16:33
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 保存save的模式
  ******************************************************************************/
object SaveModel {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(SaveModel.getClass)
    val sparkSession = SparkSession.builder().appName("SaveModel").getOrCreate()

    val dataset = sparkSession.read.json("/data/resources/people.json")

    dataset.write.mode(SaveMode.ErrorIfExists).save("tmp/people2.json") // 报错退出
    dataset.write.mode(SaveMode.Append).save("tmp/people2.json") // 追加
    dataset.write.mode(SaveMode.Ignore).save("tmp/people2.json") // 忽略错误
    dataset.write.mode(SaveMode.Overwrite).save("tmp/people2.json") // 覆盖
  }

}
