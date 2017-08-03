package com.huangyueran.spark.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @category 对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
 * @author huangyueran
 * @time 2017-7-21 17:04:50
 */
public class Join {

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

		join(sc);
	}

	static void join(JavaSparkContext sc) {
	    List<Tuple2<Integer, String>> products = new ArrayList<>();
	    products.add(new Tuple2<>(1, "苹果"));
	    products.add(new Tuple2<>(2, "梨"));
	    products.add(new Tuple2<>(3, "香蕉"));
	    products.add(new Tuple2<>(4, "石榴"));

	    List<Tuple2<Integer, Integer>> counts = new ArrayList<>();
	    counts.add(new Tuple2<>(1, 7));
	    counts.add(new Tuple2<>(2, 3));
	    counts.add(new Tuple2<>(3, 8));
	    counts.add(new Tuple2<>(4, 3));
	    counts.add(new Tuple2<>(5, 9));

	    JavaPairRDD<Integer, String> productsRDD = sc.parallelizePairs(products);
	    JavaPairRDD<Integer, Integer> countsRDD = sc.parallelizePairs(counts);

	   /**
	  	*  =================================================================================
	    *   |            对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin                    |
	    *   |            For <K, V>, and <K, W> performs join operations and returns (K, (V, W)) the outer join functions are leftOuterJoin,  | 
	    *   |            rightOuterJoin, and fullOuterJoin                                                                                                                                                     | 
	  	*   =================================================================================
	  	*/
	    productsRDD.join(countsRDD)
	            .foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
					@Override
					public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
						   System.out.println(t._1 + "\t" + t._2());
					}
				});
	}

}
