package com.huangyueran.spark.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @category 两个RDD进行笛卡尔积合并--The two RDD are Cartesian product merging
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Cartesian {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Cartesian").setMaster("local");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		cartesian(sc);
	}

	private static void cartesian(JavaSparkContext sc) {
	    List<String> names = Arrays.asList("张三", "李四", "王五");
	    List<Integer> scores = Arrays.asList(60, 70, 80);

	    JavaRDD<String> namesRDD = sc.parallelize(names);
	    JavaRDD<Integer> scoreRDD = sc.parallelize(scores);

	    /**
		 *  =====================================
		 *   |             两个RDD进行笛卡尔积合并                                        |
		 *   |             The two RDD are Cartesian product merging     |                                                                                                                                                                                                                                    | 
		 *   =====================================
		 */
	    JavaPairRDD<String, Integer> cartesianRDD = namesRDD.cartesian(scoreRDD);
	    
	    cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
	        public void call(Tuple2<String, Integer> t) throws Exception {
	            System.out.println(t._1 + "\t" + t._2());
	        }
	    });
	}
	
}
