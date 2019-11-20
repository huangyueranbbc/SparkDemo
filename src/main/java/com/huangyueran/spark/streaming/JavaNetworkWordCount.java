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

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * @author huangyueran
 * @category 自定义Spark-Streaming-WordCount
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaNetworkWordCount {

    private static final Logger log = LoggerFactory.getLogger(JavaNetworkWordCount.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        /**
         * 资源.setMaster("local[2]")必须大于1 一个负责取数据 其他负责计算
         */
//    if (args.length < 2) {
//      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
//      System.exit(1);
//    }

        StreamingExamples.setStreamingLogLevels();

        // Create the context with a 1 second batch size
        SparkConf sparkConf = SparkUtils.getLocalSparkConf(JavaNetworkWordCount.class);
        /*
         * 创建该对象类似于spark core中的JavaSparkContext
         * 该对象除了接受SparkConf对象，还接收了一个BatchInterval参数,就算说，每收集多长时间去划分一个人Batch即RDD去执行
         */
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        /*
         * 首先创建输入DStream，代表一个数据比如这里从socket或KafKa来持续不断的进入实时数据流
         * 创建一个监听Socket数据量，RDD里面的每一个元素就是一行行的文本
         */
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.2.1", 9999, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x)).iterator();
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
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
