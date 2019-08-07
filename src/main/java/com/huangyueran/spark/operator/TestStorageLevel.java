package com.huangyueran.spark.operator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * @category 缓存策略
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class TestStorageLevel {
	public static void main(String[] args) {
		// SparkConf conf = new
		// SparkConf().setAppName("TestStorageLevel").setMaster("local");
		SparkConf conf = new SparkConf().setAppName("TestStorageLevel");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 当RDD会被复用的时候通常我们需要使用持久化策略
		// 1.持久化策略默认的是MEMORY_ONLY
		// 2如果内存有些吃紧，可以选择MEMORY_ONLY_SER
		// 3.当我们的数据想要做一定的容错可以使用_2
		// 4.如果我们的中间结果RDD计算代价比较大，那我们可以选择MEMORY_AND_DISK

		// memory_only就算存不下就不存了
		// MEMORY_AND_DISK如果内存存不下会存到本地磁盘空间

		JavaRDD<String> text = sc.textFile("data/resources/test.txt");
		// text.cache(); // 做持久化 默认的持久化策略
		text.persist(StorageLevel.MEMORY_ONLY_SER());

		// 没有做持久化:3916 1258
		// 做了持久化:6041 2980 2413
		long starttime = System.currentTimeMillis();
		long count = text.count();
		System.out.println("count:" + count);
		long endtime = System.currentTimeMillis();
		System.out.println("costtime:" + (endtime - starttime));

		long starttime2 = System.currentTimeMillis();
		long count2 = text.count();
		System.out.println("count2:" + count2);
		long endtime2 = System.currentTimeMillis();
		System.out.println("costtime2:" + (endtime2 - starttime2));

		long starttime3 = System.currentTimeMillis();
		long count3 = text.count();
		System.out.println("count3:" + count3);
		long endtime3 = System.currentTimeMillis();
		System.out.println("costtime3:" + (endtime3 - starttime3));

		sc.close();
	}
}
