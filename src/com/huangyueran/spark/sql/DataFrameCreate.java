package com.huangyueran.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

/**
 * @category DataFrameCreate
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class DataFrameCreate {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameCreate").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();
		DataFrame dataFrame = dataFrameReader.json("resources/people.json");

		dataFrame.show();

		sc.close();
	}
}
