package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @category 与map类似，但每个输入的RDD成员可以产生0或多个输出成员 扁平化输出
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class FlatMap {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(FlatMap.class);

		flatMap(sc);
	}

	private static void flatMap(JavaSparkContext sc) {
		List<String> data = Arrays.asList("aa,bb,cc", "cxf,spring,struts2", "java,C++,javaScript");
		JavaRDD<String> rddData = sc.parallelize(data);

		FlatMapFunction<String, String> flatMapFunction=new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) throws Exception {
				List<String> list = Arrays.asList(s.split(","));
				return list.iterator();
			}
		};
		JavaRDD<String> flatMapData = rddData.flatMap(flatMapFunction);


		flatMapData.foreach(new VoidFunction<String>() {
			@Override
			public void call(String v) throws Exception {
				System.out.println(v);
			}
		});

		sc.close();
	}
}
