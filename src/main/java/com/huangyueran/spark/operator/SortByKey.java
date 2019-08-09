package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @category 对<key, value>结构的RDD进行升序或降序排列
 * 1.comp：排序时的比较运算方式。
 * 2.ascending：false降序；true升序。
 * 
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class SortByKey {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(SortByKey.class);

		sortByKey(sc);
	}

	private static void sortByKey(JavaSparkContext sc) {

		List<Integer> datas = Arrays.asList(60, 70, 80, 55, 45, 75);

		 /**
		 *  ====================================================================
		 *   |            sortBy对RDD进行升序或降序排列  SortBy sorts RDD in ascending or descending order                    |
		 *   |            comp：排序时的比较运算方式。ascending：false降序；true升序。                                                        |     
		 *   |            Comp: a comparative operation in sorting. Ascending:false descending; true ascending order.  |                                                                                                                                                                                                                              | 
		 *   ====================================================================
		 */
		JavaRDD<Integer> sortByRDD = sc.parallelize(datas).sortBy(new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer t) throws Exception {
				// TODO Auto-generated method stub
				return t;
			}
		}, true, 1);

		sortByRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});

		List<Tuple2<Integer, Integer>> datas2 = new ArrayList<>();
		datas2.add(new Tuple2<>(3, 3));
		datas2.add(new Tuple2<>(2, 2));
		datas2.add(new Tuple2<>(1, 4));
		datas2.add(new Tuple2<>(2, 3));


		 /**
		 *  =======================================================================
		 *   |            对<key, value>结构的RDD进行升序或降序排列--SortBy sorts RDD in ascending or descending order    |
		 *   |            The RDD of the <key and value> structures is arranged in ascending order or descending order        |     
		 *   |            Comp: a comparative operation in sorting. Ascending:false descending; true ascending order-.         |                                                                                                                                                                                                                              | 
		 *   =======================================================================
		 */
		sc.parallelizePairs(datas2).sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void call(Tuple2<Integer, Integer> v) throws Exception {
				System.out.println(v._1 + "==" + v._2);
			}
		});
	}

}
