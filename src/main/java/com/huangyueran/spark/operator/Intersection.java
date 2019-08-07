package com.huangyueran.spark.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @category 返回两个RDD的交集--Returns the intersection of two RDD 
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Intersection {

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

		intersection(sc);
	}

	static void intersection(JavaSparkContext sc) {
		List<String> datas1 = Arrays.asList("张三", "李四", "tom");
		List<String> datas2 = Arrays.asList("tom", "gim");

		/**
		 *  =====================================
		 *   |             返回两个RDD的交集                                                   |
		 *   |             Returns the intersection of two RDD                    |                                                                                                                                                                                                                                    | 
		 *   =====================================
		 */
		JavaRDD<String> intersectionRDD = sc.parallelize(datas1).intersection(sc.parallelize(datas2));

		intersectionRDD.foreach(new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	}

}
