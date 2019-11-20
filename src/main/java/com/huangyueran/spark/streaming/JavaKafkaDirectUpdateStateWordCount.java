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

import java.util.*;
import java.util.regex.Pattern;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.examples.streaming.StreamingExamples;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * @author huangyueran
 * @category KafKa作为数据源--生产者,SparkStreaming作为消费者。此方法 使用Direct创建 每次会进行增量计算 更新状态
 */
public final class JavaKafkaDirectUpdateStateWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    /**
     * @param args
     * @category updateState更新状态
     */
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamingExamples.setStreamingLogLevels();
        // SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaDirectWordCount").setMaster("local[1]");
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaDirectWordCount");
        sparkConf.setMaster(Constant.SPARK_REMOTE_SERVER_ADDRESS);
        sparkConf.set("deploy-mode", "client");
        sparkConf.setJars(new String[]{"/Users/huangyueran/ideaworkspaces1/myworkspaces/spark/SparkDemo/target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"});
        sparkConf.setIfMissing("spark.driver.host", "192.168.1.1"); // Driver地址 提交机器IP地址
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        jssc.checkpoint("."); // UpdateState必须进行checkpoint

        Map<String, String> kafkaParams = new HashMap<String, String>(); // key是topic名称,value是线程数量
        kafkaParams.put("metadata.broker.list", "master:9092"); // 指定broker在哪
        HashSet<String> topicsSet = new HashSet<String>();
        topicsSet.add("spark-kafka-test"); // 指定操作的topic

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

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

        //==========================================================================================

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

        //==========================================================================================

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }


        jssc.stop();
        jssc.close();
    }
}
