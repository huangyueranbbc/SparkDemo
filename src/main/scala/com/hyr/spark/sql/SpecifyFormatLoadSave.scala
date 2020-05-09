package com.hyr.spark.sql

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
  * @date 2019-08-30 09:42
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object SpecifyFormatLoadSave {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(SpecifyFormatLoadSave.getClass)
    val sparkSession = SparkSession.builder().appName("SpecifyFormatLoadSave").getOrCreate()

    val dataFrameReader = sparkSession.read
    val dataset = dataFrameReader.format("json").load( "/data/resources/people.json")

    // 通过关writer写入并保存save
    val write = dataset.select("name").write
    write.format("parquet").save("tmp/people.parquet")

  }

}
