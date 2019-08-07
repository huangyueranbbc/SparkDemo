package com.huangyueran.spark.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @category 对原RDD进行去重操作，返回RDD中没有重复的成员---Performs a reset operation on the original RDD and returns no duplicate members in the RDD
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Distinct {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Sample").setMaster("local");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		distinct(sc);
	}

	private static void distinct(JavaSparkContext sc) {
		List<String> datas = Arrays.asList("张三", "李四", "tom", "张三");

		 /**
		 *  ===================================
		 *   |      去重--包含shuffle操作                                                 |
		 *   |      Remove weights, including shuffle operations    |                                                                                                                                                                                                                                    | 
		 *   ===================================
		 */
		JavaRDD<String> distinctRDD = sc.parallelize(datas).distinct();
		
		distinctRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}

}
