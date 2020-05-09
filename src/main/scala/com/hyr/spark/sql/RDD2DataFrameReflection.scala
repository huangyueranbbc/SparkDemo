package com.hyr.spark.sql

import java.util
import java.util.{ArrayList, List}

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SparkSession}

/** *****************************************************************************
  *
  * @date 2019-08-29 15:33
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  * *****************************************************************************/
object RDD2DataFrameReflection {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(RDD2DataFrameReflection.getClass)

    //val sparkSession = SparkSession.builder().appName("RDD2DataFrameReflection").enableHiveSupport.getOrCreate()
    val sparkSession = SparkSession.builder().appName("RDD2DataFrameReflection").getOrCreate()

    val lineRDD = sparkContext.textFile(Constant.LOCAL_FILE_PREX + "/data/resources/people.txt")
    //val lineRDD = sparkContext.textFile("/data/resources/people.txt")
    val rowsRDD = lineRDD.map(line => {
      val str = line.split(",")
      RowFactory.create(str(0), Integer.valueOf(str(1)))
    })
    rowsRDD

    // 动态构造元数据,这里用的动态创建元数据
    // 如果不确定有哪些列，这些列需要从数据库或配置文件中加载出来!!!!
    val fields = collection.mutable.ListBuffer[StructField]()
    fields += DataTypes.createStructField("name", DataTypes.StringType, true)
    fields += DataTypes.createStructField("age", DataTypes.IntegerType, true)

    val schema = DataTypes.createStructType(fields.toArray)
    schema.printTreeString() // 打印schema

    // 根据表数据和元数据schema创建临时表
    val dataSet = sparkSession.createDataFrame(rowsRDD, schema)
    dataSet.createOrReplaceTempView("person")

    // 通过sql查询
    val persons = sparkSession.sql("select * from person")
    persons.show
    val rows = persons.collect()

    for (s <- rows) {
      println(s)
    }

    sparkContext.stop

  }

}
