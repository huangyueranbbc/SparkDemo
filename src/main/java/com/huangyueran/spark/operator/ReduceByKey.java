package com.huangyueran.spark.operator;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @category 对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作，func的类型必须是(V, V) => V     groupbykey+reduce
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class ReduceByKey {

	public static void main(String[] args) {
		/**
		 * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
		 * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
		 */
		SparkConf conf = new SparkConf().setAppName("ReduceByKey").setMaster("local");
		// SparkConf conf = new SparkConf().setAppName("JoinOperator");

		/**
		 * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
		 * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);

		reduceByKey(sc);
	}

	/**
	 * @category 统计文本单词个数
	 * @param sc
	 */
	static void reduceByKey(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile("README.md");

		/**
		 *  ====================================================================================================== 
		 *   |                                                                     根据' '分词 扁平化输出Flatten output according to space word segmentation                                                                         | 
		 *   ====================================================================================================== 
		 */
		JavaRDD<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				List<String> words = Arrays.asList(line.split(" "));
				return words.iterator();
			}
		});

		/**
		 *  ====================================================================================================== 
		 *   |                                                                   将单词转换word:1的元组格式Converts the word to the tuple format of word:1                                                                         | 
		 *   ====================================================================================================== 
		 */
		JavaPairRDD<String, Integer> wordsCount = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		/**
		 *  ========================================================================================================= 
		 *   |                                                                根据元组Key(也就是单词)来分组Grouping according to the tuple Key (that is, the word)                                                                    | 
		 *   ========================================================================================================= 
		 */
		JavaPairRDD<String, Integer> resultRDD = wordsCount.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + "\t" + t._2());
			}
		});

		sc.close();
	}

}
