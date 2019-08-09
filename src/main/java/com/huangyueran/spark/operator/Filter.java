package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @category 对元素进行过滤，对每个元素应用f函数，返回值为true的元素在RDD中保留，返回为false的将过滤掉
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Filter {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Filter.class);

		filter(sc);
	}

	private static void filter(JavaSparkContext sc) {
		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

		JavaRDD<Integer> rddData = sc.parallelize(datas);
		JavaRDD<Integer> filterRDD = rddData.filter(
				// jdk1.8
				// v1 -> v1 >= 3
				new Function<Integer, Boolean>() {
					public Boolean call(Integer v) throws Exception {
						// 过滤小于4的数
						return v >= 4;
					}
				});

		filterRDD.foreach(
				// jdk1.8
				// v -> System.out.println(v)
				new VoidFunction<Integer>() {
					@Override
					public void call(Integer integer) throws Exception {
						System.out.println(integer);
					}
				});
		sc.close();
	}

}
