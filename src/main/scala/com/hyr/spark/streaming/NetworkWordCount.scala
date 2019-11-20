package com.hyr.spark.streaming

import com.hyr.spark.utils.SparkUtils
import org.apache.spark.SparkConf
import org.apache.spark.examples.streaming.StreamingExamples
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** *****************************************************************************
  * @date 2019-11-19 2:06 PM
  * @author: <a href=mailto:huangyr>黄跃然</a>
  * @Description: 流计算SparkStreaming
  *               To run this on your local machine, you need to first run a Netcat server
  *               `$ nc -lk 9999`
  *               and then run the example
  *               `$ bin/run-example org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
  ******************************************************************************/
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()

    val sparkConf: SparkConf = SparkUtils.getLocalSparkConf(NetworkWordCount.getClass)

    /**
      * 创建该对象类似于spark core中的JavaSparkContext
      * 该对象除了接受SparkConf对象，还接收了一个BatchInterval参数,就算说，没收集多长时间去划分一个人Batch即RDD去执行
      */
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 首先创建输入DStream，代表一个数据比如这里从socket或KafKa来持续不断的进入实时数据流
      * 创建一个监听Socket数据量，RDD里面的每一个元素就是一行行的文本
      */
    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.2.1", 9999)

    val resutlStream: DStream[(String, Int)] = stream.flatMap(_.split(" ")) // 空格分隔
      .map((_, 1)).reduceByKey(_ + _)

    resutlStream.print()

    // 启动Spark计算
    ssc.start()
    // 等待计算停止
    ssc.awaitTermination()
    // 停止Spark
    ssc.stop(true)
  }

}
