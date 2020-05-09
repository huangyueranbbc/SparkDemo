package com.hyr.spark.sql

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
  * @date 2019-08-20 13:31
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  ******************************************************************************/
object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(DataFrameCreate.getClass)

    val sparkSession = SparkSession.builder().appName("DataFrameCreate").getOrCreate()
    val dataFrameReader = sparkSession.read
    val dataset = dataFrameReader.json(Constant.LOCAL_FILE_PREX + "/data/resources/people.json")
    dataset.show

    val people: Unit = dataset.createOrReplaceTempView("people")
    val frame = sparkSession.sql("show tables")
    frame.show
    val peoplesData = sparkSession sql "select * from people where age = 30"
    peoplesData.show


  }

}
