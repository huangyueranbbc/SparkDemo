package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @category 对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Filter {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Filter").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		filter(sc);
	}

	private static void filter(JavaSparkContext sc) {
		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

		JavaRDD<Integer> rddData = sc.parallelize(datas);
		JavaRDD<Integer> filterRDD = rddData.filter(
				// jdk1.8
				// v1 -> v1 >= 3
				new Function<Integer, Boolean>() {
					public Boolean call(Integer v) throws Exception {
						// 过滤小于4的数
						return v >= 4;
					}
				});

		filterRDD.foreach(
				// jdk1.8
				// v -> System.out.println(v)
				new VoidFunction<Integer>() {
					@Override
					public void call(Integer integer) throws Exception {
						System.out.println(integer);
					}
				});
		sc.close();
	}

}
