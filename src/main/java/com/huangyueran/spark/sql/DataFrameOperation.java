package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @category DataFrame的操作
 * @author huangyueran
 * @time 2019-7-24 13:58:59
 */
public class DataFrameOperation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameOperation").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext(sc);

		// 将数据源读取为数据框,可以理解为一张表。具有数据和结构信息
		Dataset<Row> dataset = sqlContext.read().json(Constant.LOCAL_FILE_PREX +"/data/resources/people.json");

		// 格式化的打印这张表
		dataset.show();

		// 搭建元数据(结构)schema
		dataset.printSchema();

		// 查询列并计算
		dataset.select("name").show();
		dataset.select(dataset.col("name"), dataset.col("age").plus(1)).show();

		// 过滤
		dataset.filter(dataset.col("age").gt(20)).show();

		// 根据某一列分组然后统计count
		dataset.groupBy("age").count().show();

		sc.close();
	}
}
