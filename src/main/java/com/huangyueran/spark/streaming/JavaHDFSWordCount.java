/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huangyueran.spark.streaming;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * @category 自定义HDFS-Spark-Streaming-WordCount
 * @author huangyueran
 *
 */
public final class JavaHDFSWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	/**
	 * To run this on your local machine, you need to first run a Netcat server
	 * `$ nc -lk 9999` and then run the example `$ bin/run-example
	 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
	 */
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local[5]");
		/*
		 * 创建该对象类似于spark core中的JavaSparkContext
		 * 该对象除了接受SparkConf对象，还接收了一个BatchInterval参数,就算说，
		 * 没收集多长时间去划分一个人Batch即RDD去执行
		 */
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		/*
		 * 首先创建输入DStream，代表一个数据比如这里从socket或KafKa来持续不断的进入实时数据流
		 * 创建一个监听Socket数据量，RDD里面的每一个元素就是一行行的文本
		 */
		JavaDStream<String> lines = ssc.textFileStream("hdfs://master:8020/wordcount_dir");

		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String x) {
				return Lists.newArrayList(SPACE.split(x)).iterator();
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.print();
		ssc.start();
		try {
			ssc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
