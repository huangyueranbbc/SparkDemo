package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  * @date 2019-08-08 10:42
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
  * *****************************************************************************/
object Join {

  def join(sparkContext: SparkContext): Unit = {
    val products = List((1,"苹果"),(2,"梨"),(3,"香蕉"),(4,"石榴"))
    val counts = List((1,7),(2,3),(3,8),(4,3),(5,9))

    val productsRDD = sparkContext.makeRDD(products)
    val countsRDD = sparkContext.makeRDD(counts)
    println("====== join ======")
    productsRDD.join(countsRDD).foreach(t=>{
      println(t)
    })
    println("====== leftOuterJoin ======")
    productsRDD.leftOuterJoin(countsRDD).foreach(t=>{
      println(t)
    })
    println("====== rightOuterJoin ======")
    productsRDD.rightOuterJoin(countsRDD).foreach(t=>{
      println(t)
    })
    println("====== fullOuterJoin ======")
    productsRDD.fullOuterJoin(countsRDD).foreach(t=>{
      println(t)
    })
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(Join.getClass)

    join(sparkContext)
  }

}
