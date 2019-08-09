package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class MapTest {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(MapTest.class);

		List<String> list = Arrays.asList("hello,bjsxt", "hello,xuruyun");

		JavaRDD<String> linesRDD = sc.parallelize(list);

		JavaRDD<Object> mapRDD = linesRDD.map(new Function<String, Object>() {

			@Override
			public Object call(String v1) throws Exception {
				return v1.split(",");
			}
		});

		JavaRDD<String> flatMapRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(",")).iterator();
			}
		});

		List<Object> collect = mapRDD.collect(); // Action算子 触发执行
		for (Object obj : collect) {
			System.out.println(obj);
		}

		List<String> collect2 = flatMapRDD.collect(); // Action算子 触发执行
		for (String s : collect2) {
			System.out.println(s);
		}
	}
}
