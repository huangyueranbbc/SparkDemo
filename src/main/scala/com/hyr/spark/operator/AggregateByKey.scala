package com.hyr.spark.operator

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}

/** *****************************************************************************
  *
  * @date 2019-08-07 15:46
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description:
  *           aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
  *           和aggregate函数类似，aggregateByKey返回值得类型不需要和RDD中value的类型一致。
  *           因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair
  *           RDD，对应的结果是Key和聚合好的值；而aggregate函数直接返回非RDD的结果。
  *           1.zeroValue：表示在每个分区中第一次拿到key值时,用于创建一个返回类型的函数,这个函数最终会被包装成先生成一个返回类型,
  *           然后通过调用seqOp函数,把第一个key对应的value添加到这个类型U的变量中。
  *           2.seqOp：这个用于把迭代分区中key对应的值添加到zeroValue创建的U类型实例中。
  *           3.combOp：这个用于合并每个分区中聚合过来的两个U类型的值。
  ******************************************************************************/
object AggregateByKey {


  def aggregateByKey(sparkContext: SparkContext): Unit = {
    val datas = List((1, 3), (2, 6), (1, 4), (2, 3))

    val rdd = sparkContext.parallelize(datas, 2)
    val tuples1 = rdd.aggregateByKey(0)((sum: Int, value: Int) => {
      println("seq:" + sum + "\t" + value)
      sum + value
    }, (sum: Int, value: Int) => {
      println("comb:" + sum + "\t" + value)
      sum + value
    }).collect()
    for (t <- tuples1) {
      println(t._1 + "   " + t._2)
    }

    val tuples2 = rdd.reduceByKey((sum: Int, value: Int) => {
      println("sum:" + sum + "\t" + "value:" + value)
      sum + value
    }).collect()
    for (t <- tuples2) {
      println(t._1 + "   " + t._2)
    }

  }

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkUtils.getLocalSparkContext(AggregateByKey.getClass)
    // val sparkContext = SparkUtils.getRemoteSparkContext(AggregateByKey.getClass) // 远程模式

    aggregateByKey(sparkContext)
  }

}
