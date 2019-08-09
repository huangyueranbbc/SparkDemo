package com.huangyueran.spark.operator;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * @author huangyueran
 * @category 缓存策略
 * @time 2019-7-21 16:38:20
 */
public class TestStorageLevel {
    public static void main(String[] args) {
        Integer mode = 1;
        if (mode == 1) {
            JavaSparkContext sc = SparkUtils.getRemoteSparkContext(TestStorageLevel.class);

            // 当RDD会被复用的时候通常我们需要使用持久化策略
            // 1.持久化策略默认的是MEMORY_ONLY
            // 2如果内存有些吃紧，可以选择MEMORY_ONLY_SER
            // 3.当我们的数据想要做一定的容错可以使用_2
            // 4.如果我们的中间结果RDD计算代价比较大，那我们可以选择MEMORY_AND_DISK

            // memory_only就算存不下就不存了
            // MEMORY_AND_DISK如果内存存不下会存到本地磁盘空间

            // 根据模式选择本地文件还是HDFS文件
            JavaRDD<String> text = sc.textFile("hyrtest/wc_data");
            // text.cache(); // 做持久化 默认的持久化策略
            text.persist(StorageLevel.MEMORY_AND_DISK_SER());

            // 没有做持久化:3916 1258
            // 做了持久化:22658 76 32
            long starttime = System.currentTimeMillis();
            long count = text.count();
            System.out.println("count:" + count);
            long endtime = System.currentTimeMillis();
            System.out.println("costtime:" + (endtime - starttime));

            long starttime2 = System.currentTimeMillis();
            long count2 = text.count();
            System.out.println("count2:" + count2);
            long endtime2 = System.currentTimeMillis();
            System.out.println("costtime2:" + (endtime2 - starttime2));

            long starttime3 = System.currentTimeMillis();
            long count3 = text.count();
            System.out.println("count3:" + count3);
            long endtime3 = System.currentTimeMillis();
            System.out.println("costtime3:" + (endtime3 - starttime3));

            sc.close();
        } else {
            JavaSparkContext sc = SparkUtils.getLocalSparkContext(TestStorageLevel.class);

            // 当RDD会被复用的时候通常我们需要使用持久化策略
            // 1.持久化策略默认的是MEMORY_ONLY
            // 2如果内存有些吃紧，可以选择MEMORY_ONLY_SER
            // 3.当我们的数据想要做一定的容错可以使用_2
            // 4.如果我们的中间结果RDD计算代价比较大，那我们可以选择MEMORY_AND_DISK

            // memory_only就算存不下就不存了
            // MEMORY_AND_DISK如果内存存不下会存到本地磁盘空间

            JavaRDD<String> text = sc.textFile(Constant.LOCAL_FILE_PREX + "test.txt");
            // text.cache(); // 做持久化 默认的持久化策略
            text.persist(StorageLevel.MEMORY_ONLY_SER());

            // 没有做持久化:3916 1258
            // 做了持久化:6041 2980 2413
            long starttime = System.currentTimeMillis();
            long count = text.count();
            System.out.println("count:" + count);
            long endtime = System.currentTimeMillis();
            System.out.println("costtime:" + (endtime - starttime));

            long starttime2 = System.currentTimeMillis();
            long count2 = text.count();
            System.out.println("count2:" + count2);
            long endtime2 = System.currentTimeMillis();
            System.out.println("costtime2:" + (endtime2 - starttime2));

            long starttime3 = System.currentTimeMillis();
            long count3 = text.count();
            System.out.println("count3:" + count3);
            long endtime3 = System.currentTimeMillis();
            System.out.println("costtime3:" + (endtime3 - starttime3));

            sc.close();
        }
    }

}
