package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * @category 合并两个RDD，不去重，要求两个RDD中的元素类型一致------逻辑上抽象的合并
 * @author huangyueran
 * @time 2017-7-21 16:38:20
 */
public class Union {

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

		union(sc);
	}

	static void union(JavaSparkContext sc ) {
	    List<String> datas1 = Arrays.asList("张三", "李四");
	    List<String> datas2 = Arrays.asList("tom", "gim");

	    JavaRDD<String> data1RDD = sc.parallelize(datas1);
	    JavaRDD<String> data2RDD = sc.parallelize(datas2);

	    /**
		 *  ====================================================================
		 *   |             合并两个RDD，不去重，要求两个RDD中的元素类型一致                                                                            |
		 *   |             Merge two RDD, -not heavy, and require the consistency of the element types in the two RDD |                                                                                                                                                                                                                                    | 
		 *   ====================================================================
		 */
	    JavaRDD<String> unionRDD = data1RDD
	            .union(data2RDD);

	    unionRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	    sc.close();
	}

}
