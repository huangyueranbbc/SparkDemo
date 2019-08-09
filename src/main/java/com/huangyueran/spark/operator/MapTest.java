package com.huangyueran.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class MapTest {
	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("JoinOperator").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> list = Arrays.asList("hello,bjsxt", "hello,xuruyun");

		JavaRDD<String> linesRDD = sc.parallelize(list);

		JavaRDD<Object> mapRDD = linesRDD.map(new Function<String, Object>() {

			@Override
			public Object call(String v1) throws Exception {
				return v1.split(",");
			}
		});

		JavaRDD<String> flatMapRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(",")).iterator();
			}
		});

		List<Object> collect = mapRDD.collect(); // Action算子 触发执行
		for (Object obj : collect) {
			System.out.println(obj);
		}

		List<String> collect2 = flatMapRDD.collect(); // Action算子 触发执行
		for (String s : collect2) {
			System.out.println(s);
		}
	}
}
