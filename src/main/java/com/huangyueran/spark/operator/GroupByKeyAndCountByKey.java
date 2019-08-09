package com.huangyueran.spark.operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Int;
import scala.Tuple2;

/**
 * @author huangyueran
 * @category 对<key, value>结构的RDD进行类似RMDB的group by聚合操作，具有相同key的RDD成员的value会被聚合在一起，返回的RDD的结构是(key, Iterator<value>)
 * @time 2019-7-21 16:38:20
 */
public class GroupByKeyAndCountByKey {

    public static void main(String[] args) {
        /**
         * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
         * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
         */
        SparkConf conf = new SparkConf().setAppName("GroupByKey").setMaster("local");

        /**
         * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
         * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
         */
        JavaSparkContext sc = new JavaSparkContext(conf);

        groupBy(sc);
    }

    static void groupBy(JavaSparkContext sc) {
        List<Integer> datas = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);

        /**
         *  =====================================================
         *   |                                                             根据奇数偶数分类                                                    |
         *   =====================================================
         */
        JavaPairRDD<Object, Iterable<Integer>> pairRDD = sc.parallelize(datas).groupBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer v1) throws Exception {
                return (v1 % 2 == 0) ? "偶数" : "奇数";
            }
        });
        List<Tuple2<Object, Iterable<Integer>>> list = pairRDD.collect();
        for (Tuple2 t : list) {
            System.out.println(t._1 + "======" + t._2);
        }

        System.out.println("==========================================================================");


        /**
         *  =====================================================
         *   |                                                         根据字符串长度分类                                                    |
         *   =====================================================
         */
        List<String> datas2 = Arrays.asList("dog", "tiger", "lion", "cat", "spider", "eagle");

        JavaPairRDD<Integer, String> pairRDD2 = sc.parallelize(datas2).keyBy(new Function<String, Integer>() {

            @Override
            public Integer call(String v) throws Exception {
                return v.length();
            }
        });

        JavaPairRDD<Integer, Iterable<Tuple2<Integer, String>>> pairRDD3 = pairRDD2
                .groupBy(new Function<Tuple2<Integer, String>, Integer>() {

                    @Override
                    public Integer call(Tuple2<Integer, String> v) throws Exception {
                        return v._1;
                    }
                });

        List<Tuple2<Integer, Iterable<Tuple2<Integer, String>>>> list2 = pairRDD3.collect();

        for (Tuple2 t : list2) {
            System.out.println(t._1 + "======" + t._2);
        }

        // countByKey
        List<Tuple2<Integer, String>> tuples = Arrays.asList(new Tuple2<Integer, String>(1, "a"),
                new Tuple2<Integer, String>(1, "b"),
                new Tuple2<Integer, String>(1, "c"),
                new Tuple2<Integer, String>(2, "d"),
                new Tuple2<Integer, String>(3, "e"));

        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(tuples);
        System.out.println(javaPairRDD.countByKey());

    }

}
