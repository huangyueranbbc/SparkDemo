package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @category 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。
 *           如果shuffle设置为true，则会进行shuffle。可以在Filter后进行Coalesce重分区来减少数据倾斜。
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Coalesce {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Coalesce").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		coalesce(sc);
	}

	private static void coalesce(JavaSparkContext sc) {
		List<String> datas = Arrays.asList("hi", "hello", "how", "are", "you");
		JavaRDD<String> datasRDD = sc.parallelize(datas, 4);
		System.out.println("RDD的分区数: " + datasRDD.partitions().size());
		JavaRDD<String> datasRDD2 = datasRDD.coalesce(2, false);
		System.out.println("RDD的分区数: " + datasRDD2.partitions().size());
	}

}
