package com.huangyueran.spark.sharedvariables;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @category 广播变量
 * @author huangyueran
 * @time 2019-7-24 10:00:05
 */
public class BroadCastValue {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getRemoteSparkContext(BroadCastValue.class);

		final int num = 3;
		// 创建广播变量
		final Broadcast<Integer> broadcastFactor = sc.broadcast(num);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		JavaRDD<Integer> resultRDD = listRDD.map(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer n) throws Exception {
				// return num * num
				// 只读的
				return n * broadcastFactor.value();
			}
		});

		resultRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer n) throws Exception {
				System.out.println(n);
			}
		});

		sc.close();

	}

}
