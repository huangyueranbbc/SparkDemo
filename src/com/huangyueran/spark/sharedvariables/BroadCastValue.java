package com.huangyueran.spark.sharedvariables;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

/**
 * @category 广播变量
 * @author huangyueran
 * @time 2017-7-24 10:00:05
 */
public class BroadCastValue {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("BroadCastValue").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		final int num = 3;
		// 创建广播变量
		final Broadcast<Integer> broadcastFactor = sc.broadcast(num);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		JavaRDD<Integer> resultRDD = listRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer n) throws Exception {
				// return num * num
				// 只读的
				return n * broadcastFactor.value();
			}
		});

		resultRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer n) throws Exception {
				System.out.println(n);
			}
		});

		sc.close();

	}

}
