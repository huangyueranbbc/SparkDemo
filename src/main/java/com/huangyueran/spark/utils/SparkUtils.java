package com.huangyueran.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/*******************************************************************************
 * @date 2019-08-09 14:50
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description: Spark工具类
 ******************************************************************************/
public class SparkUtils {

    public static JavaSparkContext getRemoteSparkContext(Class clazz) {
        System.setProperty("HADOOP_USER_NAME", "root");
        /**
         * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
         * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
         */
        SparkConf conf = new SparkConf().setAppName(clazz.getName());
        conf.setMaster(Constant.SPARK_REMOTE_SERVER_ADDRESS);
        conf.set("deploy-mode", "client");
        conf.setJars(new String[]{"/Users/huangyueran/ideaworkspaces1/myworkspaces/spark/SparkDemo/target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"});
        conf.setIfMissing("spark.driver.host", "192.168.1.1"); // Driver地址 提交机器IP地址
        /**
         * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
         * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
         */
        return new JavaSparkContext(conf);
    }

    public static JavaSparkContext getLocalSparkContext(Class clazz) {
        /**
         * SparkConf:第一步创建一个SparkConf，在这个对象里面可以设置允许模式Local Standalone yarn
         * AppName(可以在Web UI中看到) 还可以设置Spark运行时的资源要求
         */
        SparkConf conf = new SparkConf().setAppName(clazz.getName()).setMaster("local");

        /**
         * 基于SparkConf的对象可以创建出来一个SparkContext Spark上下文
         * SparkContext是通往集群的唯一通道，SparkContext在创建的时候还会创建任务调度器
         */
        return new JavaSparkContext(conf);
    }

}
