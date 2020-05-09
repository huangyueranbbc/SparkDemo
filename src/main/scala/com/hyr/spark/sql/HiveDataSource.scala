package com.hyr.spark.sql

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
  * @date 2019-08-28 16:24
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object HiveDataSource {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getRemoteSparkContext(HiveDataSource.getClass)

    val sparkSession = SparkSession.builder().appName("HiveDataSource")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://server1:18888")
      //指定hive的warehouse目录
      .config("spark.sql.warehouse.dir", "hdfs://server1:8020/user/hive/warehouse")
      .getOrCreate()

    val dataset = sparkSession.sql("show databases")
    dataset.show()

  }

}
