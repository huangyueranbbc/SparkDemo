package com.huangyueran.spark.operator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * @category 与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。
 *           所以func的类型是Iterator
 * @author huangyueran
 * @time 2017-7-21 16:38:20
 */
public class MapPartitions {

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

		mapPartitions(sc);
	}

	static void mapPartitions(JavaSparkContext sc) {
		List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4");

		JavaRDD<String> namesRDD = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsRDD = namesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			int count = 0;

			@Override
			public Iterable<String> call(Iterator<String> stringIterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while (stringIterator.hasNext()) {
					list.add("count:" + count++ + "\t" + stringIterator.next());
				}
				return list;
			}
		});

		// 从集群获取数据到本地内存中
		List<String> result = mapPartitionsRDD.collect();
		for (String s : result) {
			System.out.println(s);
		}

		sc.close();
	}

}
