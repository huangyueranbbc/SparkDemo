package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @category 与Map类似，但map中的func作用的是RDD中的每个元素，而mapPartitions中的func作用的对象是RDD的一整个分区。
 *           所以func的类型是Iterator
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class MapPartitions {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(MapPartitions.class);

		mapPartitions(sc);
	}

	private static void mapPartitions(JavaSparkContext sc) {
		List<String> names = Arrays.asList("张三1", "李四1", "王五1", "张三2", "李四2", "王五2", "张三3", "李四3", "王五3", "张三4");

		JavaRDD<String> namesRDD = sc.parallelize(names, 3);
		JavaRDD<String> mapPartitionsRDD = namesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
			int count = 0;

			@Override
			public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while (stringIterator.hasNext()) {
					list.add("count:" + count++ + "\t" + stringIterator.next());
				}
				return list.iterator();
			}
		});

		// 从集群获取数据到本地内存中
		List<String> result = mapPartitionsRDD.collect();
		for (String s : result) {
			System.out.println(s);
		}

		sc.close();
	}

}
