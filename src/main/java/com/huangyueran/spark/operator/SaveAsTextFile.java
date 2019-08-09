package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @category 将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)。路径path可以是本地路径或HDFS地址，转换方法是对RDD成员调用toString函数
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class SaveAsTextFile {
	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(SaveAsTextFile.class);

		JavaRDD<String> text = sc.textFile(Constant.LOCAL_FILE_PREX +"/data/resources/test.txt");
		JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// 统计词出现次数
		JavaPairRDD<String, Integer> results = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});

		// 键值对互换
		JavaPairRDD<Integer, String> temp = results
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
						return new Tuple2<Integer, String>(tuple._2, tuple._1);
					}
				});

		// 排序
		JavaPairRDD<String, Integer> sorted = temp.sortByKey(false)
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
						return new Tuple2<String, Integer>(tuple._2, tuple._1);
					}
				});

		List<Tuple2<String, Integer>> list = sorted.collect();

		sorted.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("word:" + tuple._1 + "\tcount:" + tuple._2);
			}
		});

		for (Tuple2<String, Integer> t : list) {
			System.out.println(t._1 + "======" + t._2);
		}
		
		/**
		 *  ==================================================================================
		 *   |       saveAsTextFile-将RDD转换为文本内容并保存至路径path下，可能有多个文件(和partition数有关)。                                                |
		 *   |       Convert RDD to text content and save to path path. There may be multiple files (related to the number of partition).      |                                                                                                                                                                                                                                    | 
		 *   ==================================================================================
		 */
		sorted.saveAsTextFile(Constant.LOCAL_FILE_PREX +"tmp/wordcount_result");

		sc.close();
	}
}
