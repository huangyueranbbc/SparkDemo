package com.huangyueran.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;

/**
 * @category LoadSave
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class LoadSave {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();

		// parquet 是本地数据存储的格式
		DataFrame df = dataFrameReader.load("resources/users.parquet");

		df.printSchema();
		df.show();

		// 通过关writer写入并保存save
		DataFrameWriter write = df.select("name", "favorite_color").write();
		write.save("namesAndColors.parquet");

		// DataFrame df1 = dataFrameReader
		// .load("namesAndColors.parquet/part-r-00000-7de940b5-233f-43b5-8e4f-1ef80390f28b.gz.parquet");
		// df1.show();

		sc.close();
	}
}
