package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @category 对原RDD进行去重操作，返回RDD中没有重复的成员---Performs a reset operation on the original RDD and returns no duplicate members in the RDD
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Distinct {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Distinct.class);

		distinct(sc);
	}

	private static void distinct(JavaSparkContext sc) {
		List<String> datas = Arrays.asList("张三", "李四", "tom", "张三");

		 /**
		 *  ===================================
		 *   |      去重--包含shuffle操作                                                 |
		 *   |      Remove weights, including shuffle operations    |                                                                                                                                                                                                                                    | 
		 *   ===================================
		 */
		JavaRDD<String> distinctRDD = sc.parallelize(datas).distinct();
		
		distinctRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}

}
