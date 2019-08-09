package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @category 对RDD进行抽样，其中参数withReplacement为true时表示抽样之后还放回，可以被多次抽样，false表示不放回；fraction表示抽样比例；seed为随机数种子，比如当前时间戳
 * @author huangyueran
 * @time 2019-7-21 16:38:20
 */
public class SampleAndTake {

	public static void main(String[] args) {
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(SampleAndTake.class);

		sample(sc);
	}

	static void sample(JavaSparkContext sc) {
		List<Integer> datas = Arrays.asList(1, 2, 3, 7, 4, 5, 8);

		JavaRDD<Integer> dataRDD = sc.parallelize(datas);
		
		/**
		 *  ====================================================================================================== 
		 *   |                   随机抽样-----参数withReplacement为true时表示抽样之后还放回,可以被多次抽样,false表示不放回;fraction表示抽样比例;seed为随机数种子                       |
		 *   |                   The random  sampling parameter withReplacement is true, which means that after sampling, it can be returned. It can be sampled many times,  |
		 *   |                   and false indicates no return.  Fraction represents the sampling proportion;seed is the random number seed                                                               |                                                                                                                                                                                                                                           | 
		 *   ====================================================================================================== 
		 */
		JavaRDD<Integer> sampleRDD = dataRDD.sample(false, 0.5, System.currentTimeMillis());
		
		// TODO dataRDD.takeSample(false, 3);
		// TODO dataRDD.take(3)

		sampleRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});

		sc.close();
	}

}
