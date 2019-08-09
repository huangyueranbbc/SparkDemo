package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @category 返回两个RDD的交集--Returns the intersection of two RDD 
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Intersection {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Intersection.class);

		intersection(sc);
	}

	static void intersection(JavaSparkContext sc) {
		List<String> datas1 = Arrays.asList("张三", "李四", "tom");
		List<String> datas2 = Arrays.asList("tom", "gim");

		/**
		 *  =====================================
		 *   |             返回两个RDD的交集                                                   |
		 *   |             Returns the intersection of two RDD                    |                                                                                                                                                                                                                                    | 
		 *   =====================================
		 */
		JavaRDD<String> intersectionRDD = sc.parallelize(datas1).intersection(sc.parallelize(datas2));

		intersectionRDD.foreach(new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	}

}
