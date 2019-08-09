package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @category 合并两个RDD，不去重，要求两个RDD中的元素类型一致------逻辑上抽象的合并
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class Union {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(Union.class);

		union(sc);
	}

	static void union(JavaSparkContext sc ) {
	    List<String> datas1 = Arrays.asList("张三", "李四");
	    List<String> datas2 = Arrays.asList("tom", "gim");

	    JavaRDD<String> data1RDD = sc.parallelize(datas1);
	    JavaRDD<String> data2RDD = sc.parallelize(datas2);

	    /**
		 *  ====================================================================
		 *   |             合并两个RDD，不去重，要求两个RDD中的元素类型一致                                                                            |
		 *   |             Merge two RDD, -not heavy, and require the consistency of the element types in the two RDD |                                                                                                                                                                                                                                    | 
		 *   ====================================================================
		 */
	    JavaRDD<String> unionRDD = data1RDD
	            .union(data2RDD);

	    unionRDD.foreach(new VoidFunction<String>() {
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

	    sc.close();
	}

}
