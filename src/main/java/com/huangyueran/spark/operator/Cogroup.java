package com.huangyueran.spark.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.Tuple3;

/**
 * @category 对多个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
 * @author huangyueran
 * @time 2019-7-21 16:52:49
 */
public class Cogroup {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Cogroup").setMaster("local");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		cogroup(sc);
	}

	static void cogroup(JavaSparkContext sc) {
		List<Tuple2<Integer, String>> datas1 = new ArrayList<>();
		datas1.add(new Tuple2<>(1, "苹果"));
		datas1.add(new Tuple2<>(2, "梨"));
		datas1.add(new Tuple2<>(3, "香蕉"));
		datas1.add(new Tuple2<>(4, "石榴"));

		List<Tuple2<Integer, Integer>> datas2 = new ArrayList<>();
		datas2.add(new Tuple2<>(1, 7));
		datas2.add(new Tuple2<>(2, 3));
		datas2.add(new Tuple2<>(3, 8));
		datas2.add(new Tuple2<>(4, 3));

		List<Tuple2<Integer, String>> datas3 = new ArrayList<>();
		datas3.add(new Tuple2<>(1, "7"));
		datas3.add(new Tuple2<>(2, "3"));
		datas3.add(new Tuple2<>(3, "8"));
		datas3.add(new Tuple2<>(4, "3"));
		datas3.add(new Tuple2<>(4, "4"));
		datas3.add(new Tuple2<>(4, "5"));
		datas3.add(new Tuple2<>(4, "6"));

		/**
		 *   ===================================================== =========================
		 *   |    Cogroup: groups the elements in the same key in each RDD into a collection of KV elements in each RDD.                   |
		 *   |    Unlike reduceByKey, the elements of the same key are merged in the two RDD.                                                                   | 
		 *   ===============================================================================
		 */
		sc.parallelizePairs(datas1).cogroup(sc.parallelizePairs(datas2), sc.parallelizePairs(datas3)).foreach(
				new VoidFunction<Tuple2<Integer, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<String>>>>() {
					@Override
					public void call(Tuple2<Integer, Tuple3<Iterable<String>, Iterable<Integer>, Iterable<String>>> t)
							throws Exception {
						System.out.println(t._1 + "==" + t._2);
					}
				});
	}

}
