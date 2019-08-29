package com.hyr.spark.sql

import java.io.Serializable

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/** *****************************************************************************
  * @date 2019-08-29 15:52
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 通过反射动态转换RDD和DataFrame
  ******************************************************************************/
object RDD2DataFrameReflectionDynamic {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(RDD2DataFrameReflectionDynamic.getClass)
    val sparkSession = SparkSession.builder().appName("RDD2DataFrameReflectionDynamic").getOrCreate()

    val lineRDD = sparkContext.textFile(Constant.LOCAL_FILE_PREX + "/data/resources/people.txt")

    val personsRDD = lineRDD.map(line => {
      val parts = line.split(",")
      val person = new Person
      person.setName(parts(0))
      person.setAge(parts(1).trim.toInt)
      person
    })

    // 通过反射方式将RDD转换为DataFrame
    val personDataset: Dataset[Row] = sparkSession.createDataFrame(personsRDD, classOf[RDD2DataFrameReflectionDynamic.Person]) // RDD数据,格式Schem
    personDataset.createTempView("person")
    val teenagers = sparkSession.sql("select * from person where age >= 13 and age <= 19")
    teenagers.printSchema()
    val persons = teenagers.rdd.map(row => {

      val age:Int = row.getAs("age")
      val name:String = row.getAs("name")
      val person = new Person
      person.setName(name)
      person.setAge(age)
      person
    }).collect
    persons.foreach(println)
  }

  class Person extends Serializable {
    private var name: String = _
    private var age: Int = 0

    def getName: String = name

    def setName(name: String): Unit = {
      this.name = name
    }

    def getAge: Int = age

    def setAge(age: Int): Unit = {
      this.age = age
    }

    override def toString: String = "Person [name=" + name + ", age=" + age + "]"
  }

}
