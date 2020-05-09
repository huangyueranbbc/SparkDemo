package com.hyr.spark.utils

import com.huangyueran.spark.utils.Constant
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-09 15:11
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 工具类 封装一些通用方法 方便调试
  ******************************************************************************/
object SparkUtils {

  def getRemoteSparkContext(clazz: Class[_]): SparkContext = {
    System.setProperty("HADOOP_USER_NAME", "root")
    /**
      * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
      * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
      */
    val conf = new SparkConf setAppName clazz.getName setMaster Constant.SPARK_REMOTE_SERVER_ADDRESS

    // 设置catelog类型 支持hive和in memory。围绕数据库、表和函数三种实体，提供创建、检索、缓存数据和删除的功能。
    conf.set("spark.sql.catalogImplementation","hive")

    //conf.setJars(Array[String]("/Users/huangyueran/ideaworkspaces1/myworkspaces/spark/SparkDemo/target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    //conf.setIfMissing("spark.driver.host", "192.168.1.128")

    /**
      * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
      * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
      */
    new SparkContext(conf)
  }

  def getLocalSparkContext(clazz: Class[_]): SparkContext = {
    System.setProperty("HADOOP_USER_NAME", "root")
    /**
      * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
      * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
      */
    val conf = new SparkConf setAppName clazz.getName setMaster "local"
    conf.setSparkHome(".")
    /**
      * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
      * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
      */
    new SparkContext(conf)
  }

  def getRemoteSparkConf(clazz: Class[_]): SparkConf = {
    val conf = new SparkConf().setAppName(clazz.getName)
    conf.setMaster(Constant.SPARK_REMOTE_SERVER_ADDRESS)
    conf.set("deploy-mode", "client")
    //conf.setJars(Array[String]("/Users/huangyueran/ideaworkspaces1/myworkspaces/spark/SparkDemo/target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    conf.setIfMissing("spark.driver.host", "192.168.1.128") // Driver地址 提交机器IP地址

    conf
  }

  def getLocalSparkConf(clazz: Class[_]): SparkConf = new SparkConf().setAppName(clazz.getName).setMaster("local")

}
