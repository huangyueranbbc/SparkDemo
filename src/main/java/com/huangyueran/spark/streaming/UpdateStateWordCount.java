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

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.Optional;
import com.google.common.collect.Lists;
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
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author huangyueran
 * @category updateState更新状态
 */
public final class UpdateStateWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        StreamingExamples.setStreamingLogLevels();
        System.setProperty("HADOOP_USER_NAME", "root");

        // Create the context with a 1 second batch size
        SparkConf sparkConf = SparkUtils.getRemoteSparkConf(UpdateStateWordCount.class);
        /*
         * 创建该对象类似于spark core中的JavaSparkContext
         * 该对象除了接受SparkConf对象，还接收了一个BatchInterval参数,就算说，
         * 没收集多长时间去划分一个人Batch即RDD去执行
         */
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        ssc.checkpoint("./checkpoint/"); //  checkpoint 将 RDD 持久化到 HDFS 或本地文件夹

        /*
         * 首先创建输入DStream，代表一个数据比如这里从socket或KafKa来持续不断的进入实时数据流
         * 创建一个监听Socket数据量，RDD里面的每一个元素就是一行行的文本
         */
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("192.168.2.1", 9999,
                StorageLevels.MEMORY_AND_DISK_SER);


        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Lists.newArrayList(SPACE.split(s)).iterator();
            }
        };

        JavaDStream<String> words = lines.flatMap(flatMapFunction);

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        /*
         * 会不断取出上一次的状态 然后进行更新
         */
        JavaPairDStream<String, Integer> updateStateCounts = wordCounts.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
                if (state.isPresent()) { // 如果有上一次的状态
                    newValue = state.get();
                }
                for (Integer value : values) {
                    newValue += value;
                }

                return Optional.of(newValue);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        updateStateCounts.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }

        ssc.stop();
        ssc.close();
    }
}
