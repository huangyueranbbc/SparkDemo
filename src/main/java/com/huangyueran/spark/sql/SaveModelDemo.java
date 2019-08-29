package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @category 保存save的模式
 * @author huangyueran
 * @time 2019-7-24 13:58:59
 */
public class SaveModelDemo {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SaveModelDemo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// 创建DataFrame 读取json
		SQLContext sqlContext = new SQLContext(sc);

		Dataset<Row> dataset = sqlContext.read().format("json").load(Constant.LOCAL_FILE_PREX +"/data/resources/people.json");

		dataset.write().mode(SaveMode.ErrorIfExists).save("tmp/people2.json"); // 报错退出
		dataset.write().mode(SaveMode.Append).save("tmp/people2.json"); // 追加
		dataset.write().mode(SaveMode.Ignore).save("tmp/people2.json"); // 忽略错误
		dataset.write().mode(SaveMode.Overwrite).save("tmp/people2.json");// 覆盖

		sc.close();
	}
}
