package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @category 对RDD成员使用func进行reduce操作，func接受两个参数，合并之后只返回一个值。reduce操作的返回结果只有一个值。需要注意的是，func会并发执行
 * @author huangyueran
 * @time 2017-7-21 16:38:20
 */
public class Reduce {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("Reduce").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		reduce(sc);
	}

	private static void reduce(JavaSparkContext sc) {
		
		List<Integer> numberList=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> javaRDD = sc.parallelize(numberList);
		
		/**
		 *  ===================================================== 
		 *   |                                                                 累加求和                                                               | 
		 *   =====================================================
		 */
		Integer num = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
			/**
			 * @param num1上一次计算结果 return的值
			 * @param num2 当前值
			 */
			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				// System.out.println(num1+"======"+num2);
				return num1 + num2;
			}
		});
		
		System.out.println(num);
		
		sc.close();
	}

}
