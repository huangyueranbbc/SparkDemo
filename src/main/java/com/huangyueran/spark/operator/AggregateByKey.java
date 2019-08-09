package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huangyueran
 * @category aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
 * 和aggregate函数类似，aggregateByKey返回值得类型不需要和RDD中value的类型一致。
 * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair
 * RDD，对应的结果是Key和聚合好的值；而aggregate函数直接返回非RDD的结果。
 * 1.zeroValue：表示在每个分区中第一次拿到key值时,用于创建一个返回类型的函数,这个函数最终会被包装成先生成一个返回类型,
 * 然后通过调用seqOp函数,把第一个key对应的value添加到这个类型U的变量中。
 * 2.seqOp：这个用于把迭代分区中key对应的值添加到zeroValue创建的U类型实例中。
 * 3.combOp：这个用于合并每个分区中聚合过来的两个U类型的值。
 * @time 2019-7-21 16:38:20
 */
public class AggregateByKey {

    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(AggregateByKey.class);
        // JavaSparkContext sc = SparkUtils.getRemoteSparkContext("AggregateByKey");

        aggregateByKey(sc);
    }

    private static void aggregateByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
        datas.add(new Tuple2<>(1, 3));
        datas.add(new Tuple2<>(1, 2));
        datas.add(new Tuple2<>(1, 4));
        datas.add(new Tuple2<>(2, 3));

        List<Tuple2<Integer, Integer>> list = sc.parallelizePairs(datas, 2)
                .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        System.out.println("seq: " + v1 + "\t" + v2);
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        System.out.println("comb: " + v1 + "\t" + v2);
                        return v1 + v2;
                    }
                }).collect();

        List<Tuple2<Integer, Integer>> list2 = sc.parallelizePairs(datas, 2)
                .reduceByKey(new Function2<Integer, Integer, Integer>() {

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }).collect();

        for (Tuple2 t : list) {
            System.out.println(t._1 + "=====" + t._2);
        }

        for (Tuple2 t : list2) {
            System.out.println(t._1 + "=====" + t._2);
        }
    }

}
