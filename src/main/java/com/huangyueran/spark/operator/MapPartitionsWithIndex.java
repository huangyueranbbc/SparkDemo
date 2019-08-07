package com.huangyueran.spark.operator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @category 与mapPartitions类似，但输入会多提供一个整数表示分区的编号，所以func的类型是(Int, Iterator)
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class MapPartitionsWithIndex {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("RepartitionAndSortWithinPartitions").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		mapPartitionsWithIndex(sc);
	}

	private static void mapPartitionsWithIndex(JavaSparkContext sc) {

		List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4");

		// 初始化，分为3个分区
		JavaRDD<String> namesRDD = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsWithIndexRDD = namesRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

					private static final long serialVersionUID = 1L;

					public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
						List<String> list = new ArrayList<String>();
						while (v2.hasNext()) {
							list.add("分区索引:" + v1 + "\t" + v2.next());
						}
						return list.iterator();
					}
				}, true);

		// 从集群获取数据到本地内存中
		List<String> result = mapPartitionsWithIndexRDD.collect();
		for (String s : result) {
			System.out.println(s);
		}

		sc.close();
	}

}
