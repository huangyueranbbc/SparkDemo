package com.hyr.spark.sql

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession

/** *****************************************************************************
  * @date 2019-08-20 13:45
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: DataFrame的操作
  ******************************************************************************/
object DataFrameOperation {

  def main(args: Array[String]): Unit = {

    val sparkContext = SparkUtils.getLocalSparkContext(DataFrameOperation.getClass)

    val sparkSession = SparkSession.builder().appName("DataFrameOperation").getOrCreate()
    val dataset = sparkSession.read.json(Constant.LOCAL_FILE_PREX + "/data/resources/people.json")

    dataset.show

    dataset.printSchema

    dataset select "name" show

    // 查询列并计算
    dataset select(dataset.col("name"), dataset.col("age").plus(1)) show

    // 过滤
    dataset.filter(dataset.col("age").gt(20)).show()

    dataset.groupBy("age").count().show()
  }

}
