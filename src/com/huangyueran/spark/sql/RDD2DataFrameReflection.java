package com.huangyueran.spark.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @category 动态创建schema元数据
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class RDD2DataFrameReflection {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> lineRDD = sc.textFile("resources/people.txt");

		JavaRDD<Row> rowsRDD = lineRDD.map(new Function<String, Row>() {

			@Override
			public Row call(String line) throws Exception {
				String[] lineSplited = line.split(",");

				return RowFactory.create(lineSplited[0], Integer.valueOf(lineSplited[1]));
			}
		});

		// 动态构造元数据,这里用的动态创建元数据
		// 如果不确定有哪些列，这些列需要从数据库或配置文件中加载出来!!!!
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

		StructType schema = DataTypes.createStructType(fields);

		// 根据表数据和元数据schema创建临时表
		DataFrame dataFrame = sqlContext.createDataFrame(rowsRDD, schema);
		dataFrame.registerTempTable("person");

		DataFrame personDataFrame = sqlContext.sql("select * from person");

		List<Row> list = personDataFrame.javaRDD().collect();

		// 一行记录
		for (Row r : list) {
			System.out.println(r);
		}

		sc.close();
	}

	public static class Person implements Serializable {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public String toString() {
			return "Person [name=" + name + ", age=" + age + "]";
		}

	}
}
