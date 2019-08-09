package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @category 对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin
 * @author huangyueran
 * @time 2019-7-21 17:04:50
 */
public class Join {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Join.class);

		join(sc);
	}

	static void join(JavaSparkContext sc) {
	    List<Tuple2<Integer, String>> products = new ArrayList<>();
	    products.add(new Tuple2<>(1, "苹果"));
	    products.add(new Tuple2<>(2, "梨"));
	    products.add(new Tuple2<>(3, "香蕉"));
	    products.add(new Tuple2<>(4, "石榴"));

	    List<Tuple2<Integer, Integer>> counts = new ArrayList<>();
	    counts.add(new Tuple2<>(1, 7));
	    counts.add(new Tuple2<>(2, 3));
	    counts.add(new Tuple2<>(3, 8));
	    counts.add(new Tuple2<>(4, 3));
	    counts.add(new Tuple2<>(5, 9));

	    JavaPairRDD<Integer, String> productsRDD = sc.parallelizePairs(products);
	    JavaPairRDD<Integer, Integer> countsRDD = sc.parallelizePairs(counts);

	   /**
	  	*  =================================================================================
	    *   |            对<K, V>和<K, W>进行join操作，返回(K, (V, W))外连接函数为leftOuterJoin、rightOuterJoin和fullOuterJoin                    |
	    *   |            For <K, V>, and <K, W> performs join operations and returns (K, (V, W)) the outer join functions are leftOuterJoin,  | 
	    *   |            rightOuterJoin, and fullOuterJoin                                                                                                                                                     | 
	  	*   =================================================================================
	  	*/
	    productsRDD.join(countsRDD)
	            .foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
					@Override
					public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
						   System.out.println(t._1 + "\t" + t._2());
					}
				});
	}

}
