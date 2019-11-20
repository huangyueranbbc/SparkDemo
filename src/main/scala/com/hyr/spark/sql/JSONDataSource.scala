package com.hyr.spark.sql

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

/** *****************************************************************************
  * @date 2019-08-29 10:42
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object JSONDataSource {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(JSONDataSource.getClass)

    val sparkSession = SparkSession.builder().appName("JSONDataSource").getOrCreate()
    val dataFrameReader = sparkSession.read
//    val dataset = dataFrameReader.json("/data/resources/people.json")
    val dataset = dataFrameReader.json(Constant.LOCAL_FILE_PREX + "/data/resources/people.json")
    dataset.printSchema

    // 注册
    dataset.createTempView("people")
    val teenagers = sparkSession.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    val names = teenagers.rdd.map(row => {
      "name:" + row.getString(0)
    })
    names.foreach(println)

    dataset.write.format("json").mode(SaveMode.Overwrite).save("tmp/student")


  }

}
