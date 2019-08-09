package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @category 与mapPartitions类似，但输入会多提供一个整数表示分区的编号，所以func的类型是(Int, Iterator)
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class MapPartitionsWithIndex {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getRemoteSparkContext(MapPartitionsWithIndex.class);

		mapPartitionsWithIndex(sc);
	}

	private static void mapPartitionsWithIndex(JavaSparkContext sc) {

		List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4");

		// 初始化，分为3个分区
		JavaRDD<String> namesRDD = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsWithIndexRDD = namesRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

					private static final long serialVersionUID = 1L;

					public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
						List<String> list = new ArrayList<String>();
						while (v2.hasNext()) {
							list.add("分区索引:" + v1 + "\t" + v2.next());
						}
						return list.iterator();
					}
				}, true);

		// 从集群获取数据到本地内存中
		List<String> result = mapPartitionsWithIndexRDD.collect();
		for (String s : result) {
			System.out.println(s);
		}

		sc.close();
	}

}
