package com.hyr.spark.operator

import com.huangyueran.spark.utils.Constant
import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-09 11:04
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)。路径path可以是本地路径或HDFS地址，转换方法是对RDD成员调用toString函数
  * *****************************************************************************/
object SaveAsTextFile {

  def saveAsTextFile(sparkContext: SparkContext): Unit = {

    val textRDD = sparkContext.textFile(Constant.LOCAL_FILE_PREX+"/data/resources/test.txt")
    val wordsRdd = textRDD.flatMap(line => {
      line.split(" ")
    })
    val wordcount = wordsRdd.map(word => {
      (word, 1)
    })

    val resultRDD = wordcount.reduceByKey(_+_)
    resultRDD.foreach(println)

    // 根据出现次数排序 从高到底
    val sortRDD = resultRDD.sortBy(tuple => {
      tuple._2
    },ascending = false)
    sortRDD.foreach(println)

    sortRDD.saveAsTextFile(Constant.LOCAL_FILE_PREX+"/tmp/wordcount_result")


  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(SaveAsTextFile.getClass)

    saveAsTextFile(sparkContext)

  }

}
