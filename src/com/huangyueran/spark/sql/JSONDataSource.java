package com.huangyueran.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @category JSON数据源
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class JSONDataSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JSONDataSource").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		DataFrameReader dataFrameReader = sqlContext.read();
		DataFrame df = dataFrameReader.format("json").load("resources/people.json");
		df.printSchema();

		// 注册一张临时表
		df.registerTempTable("people");

		DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");
		List<String> list = teenagers.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		for (String name : list) {
			System.out.println(name);
		}

		// 创建数据源
		List<String> personInfoJSONs = new ArrayList<String>();
		personInfoJSONs.add("{\"name\":\"ZhangFa\",\"age\":32}");
		personInfoJSONs.add("{\"name\":\"Faker\",\"age\":12}");
		personInfoJSONs.add("{\"name\":\"Moon\",\"age\":62}");

		// 根据数据源创建临时表
		JavaRDD<String> studentInfosRDD = sc.parallelize(personInfoJSONs);
		DataFrame studentDataFrame = sqlContext.read().format("json").json(studentInfosRDD);
		studentDataFrame.registerTempTable("student");

		DataFrame dataFrame = sqlContext.sql("select * from student");
		dataFrame.javaRDD().foreach(new VoidFunction<Row>() {
			@Override
			public void call(Row row) throws Exception {
				System.out.println(row);
			}
		});

		dataFrame.write().format("json").mode(SaveMode.Overwrite).save("student");

		sc.close();
	}
}
