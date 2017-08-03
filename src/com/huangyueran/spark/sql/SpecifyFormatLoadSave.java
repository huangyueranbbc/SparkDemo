package com.huangyueran.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;

/**
 * @category format()简单格式的格式转换存储
 * @author huangyueran
 * @time 2017-7-24 14:33:23
 */
public class SpecifyFormatLoadSave {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SpecifyFormatLoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();

		// parquet 是本地数据存储的格式
		DataFrame df = dataFrameReader.format("json").load("resources/people.json");

		// 通过关writer写入并保存save
		DataFrameWriter write = df.select("name").write();
		write.format("parquet").save("people.parquet");

		sc.close();
	}
}
