package com.huangyueran.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @category 保存save的模式
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class SaveModelDemo {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveModelDemo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		DataFrame dataFrame = sqlContext.read().format("json").load("resources/people.json");

		dataFrame.write().mode(SaveMode.ErrorIfExists).save("people2.json"); // 报错退出
		dataFrame.write().mode(SaveMode.Append).save("people2.json"); // 追加
		dataFrame.write().mode(SaveMode.Ignore).save("people2.json"); // 忽略错误
		dataFrame.write().mode(SaveMode.Overwrite).save("people2.json");// 覆盖

		sc.close();
	}
}
