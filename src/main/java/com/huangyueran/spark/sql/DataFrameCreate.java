package com.huangyueran.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @category DataFrameCreate
 * @author huangyueran
 * @time 2019-7-24 13:58:59
 */
public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();
		Dataset<Row> dataset = dataFrameReader.json("/data/resources/people.json");

		dataset.show();

		sc.close();
	}
}
