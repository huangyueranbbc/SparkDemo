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
 * @category 对<key, value>结构的RDD进行聚合，对具有相同key的value调用func来进行reduce操作，func的类型必须是(V, V) => V     groupbykey+reduce
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class ReduceByKey {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(ReduceByKey.class);

		reduceByKey(sc);
	}

	/**
	 * @category 统计文本单词个数
	 * @param sc
	 */
	private static void reduceByKey(JavaSparkContext sc) {
		JavaRDD<String> lines = sc.textFile(Constant.LOCAL_FILE_PREX +"README.md");

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
