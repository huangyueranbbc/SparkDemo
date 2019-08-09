package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @category 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。
 *           如果shuffle设置为true，则会进行shuffle。可以在Filter后进行Coalesce重分区来减少数据倾斜。
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Coalesce {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Coalesce.class);

		coalesce(sc);
	}

	private static void coalesce(JavaSparkContext sc) {
		List<String> datas = Arrays.asList("hi", "hello", "how", "are", "you");
		JavaRDD<String> datasRDD = sc.parallelize(datas, 4);
		System.out.println("RDD的分区数: " + datasRDD.partitions().size());
		JavaRDD<String> datasRDD2 = datasRDD.coalesce(2, false);
		System.out.println("RDD的分区数: " + datasRDD2.partitions().size());
	}

}
