package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @category 两个RDD进行笛卡尔积合并--The two RDD are Cartesian product merging
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Cartesian {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Cartesian.class);

		cartesian(sc);
	}

	private static void cartesian(JavaSparkContext sc) {
	    List<String> names = Arrays.asList("张三", "李四", "王五");
	    List<Integer> scores = Arrays.asList(60, 70, 80);

	    JavaRDD<String> namesRDD = sc.parallelize(names);
	    JavaRDD<Integer> scoreRDD = sc.parallelize(scores);

	    /**
		 *  =====================================
		 *   |             两个RDD进行笛卡尔积合并                                        |
		 *   |             The two RDD are Cartesian product merging     |                                                                                                                                                                                                                                    | 
		 *   =====================================
		 */
	    JavaPairRDD<String, Integer> cartesianRDD = namesRDD.cartesian(scoreRDD);
	    
	    cartesianRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
	        public void call(Tuple2<String, Integer> t) throws Exception {
	            System.out.println(t._1 + "\t" + t._2());
	        }
	    });
	}
	
}
