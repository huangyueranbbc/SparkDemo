package com.hyr.spark.sql

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
  * @date 2019-08-29 15:19
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object LoadSave {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(LoadSave.getClass)

    val sparkSession = SparkSession.builder().appName("LoadSave").getOrCreate()

    val dataSet = sparkSession.read.load(Constant.LOCAL_FILE_PREX+"/data/resources/users.parquet")
    dataSet.printSchema
    dataSet.show

    val write = dataSet.select("name", "favorite_color", "favorite_numbers").write
    // 通过关writer写入并保存save
    write.save(Constant.LOCAL_FILE_PREX+"tmp/namesAndColors.parquet")



  }

}
