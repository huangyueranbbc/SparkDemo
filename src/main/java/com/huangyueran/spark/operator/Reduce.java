package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @category 对RDD成员使用func进行reduce操作，func接受两个参数，合并之后只返回一个值。reduce操作的返回结果只有一个值。需要注意的是，func会并发执行
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Reduce {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Reduce.class);

		reduce(sc);
	}

	private static void reduce(JavaSparkContext sc) {
		
		List<Integer> numberList=Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		JavaRDD<Integer> javaRDD = sc.parallelize(numberList);
		
		/**
		 *   =====================================================
		 *   |                                                                 累加求和                                                               | 
		 *   =====================================================
		 */
		Integer num = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
			/**
			 * @param num1上一次计算结果 return的值
			 * @param num2 当前值
			 */
			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				// System.out.println(num1+"======"+num2);
				return num1 + num2;
			}
		});
		
		System.out.println(num);
		
		sc.close();
	}

}
