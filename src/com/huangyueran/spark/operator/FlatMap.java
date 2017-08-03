package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.tools.sysinfo;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @category 与map类似，但每个输入的RDD成员可以产生0或多个输出成员 扁平化输出
 * @author huangyueran
 * @time 2017-7-21 16:38:20
 */
public class FlatMap {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("FlatMap").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		flatMap(sc);
	}

	static void flatMap(JavaSparkContext sc) {
		List<String> data = Arrays.asList("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript");
		JavaRDD<String> rddData = sc.parallelize(data);
		JavaRDD<String> flatMapData = rddData.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String t) throws Exception {
				List<String> list = Arrays.asList(t.split(","));
				return list;
			}
		});

		flatMapData.foreach(new VoidFunction<String>() {
			@Override
			public void call(String v) throws Exception {
				System.out.println(v);
			}
		});

		sc.close();
	}
}
