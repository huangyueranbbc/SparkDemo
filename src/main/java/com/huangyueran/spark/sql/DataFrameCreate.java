package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
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
		JavaSparkContext sc = SparkUtils.getLocalSparkContext(DataFrameCreate.class);

		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();
		Dataset<Row> dataset = dataFrameReader.json(Constant.LOCAL_FILE_PREX +"/data/resources/people.json");

		dataset.show();

		sc.close();
	}
}
