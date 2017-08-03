package com.huangyueran.spark.sql;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.examples.sql.JavaSparkSQL.Person;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @category 通过反射转换RDD和DataFrame
 * @author huangyueran
 * @time 2017-7-24 13:58:59
 */
public class RDD2DataFrameReflectionDynamic {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		JavaRDD<String> lineRDD = sc.textFile("resources/people.txt");

		JavaRDD<Person> personsRDD = lineRDD.map(new Function<String, Person>() {

			@Override
			public Person call(String line) throws Exception {
				String[] parts = line.split(",");
				Person person = new Person();
				person.setName(parts[0]);
				person.setAge(Integer.parseInt(parts[1].trim()));

				return person;
			}
		});

		// 通过反射方式将RDD转换为DataFrame
		DataFrame schemaPeople = sqlContext.createDataFrame(personsRDD, Person.class); // RDD数据,格式Schem
		schemaPeople.printSchema();
		// 有了DataFrame可以注册一个临时表, 使用SQL语句查询
		schemaPeople.registerTempTable("people");
		DataFrame teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19");

		// 将DataFrame转换为RDD
		JavaRDD<Row> javaRDD = teenagers.toJavaRDD();

		List<Person> persons = javaRDD.map(new Function<Row, Person>() {
			@Override
			public Person call(Row row) {
				String name = row.getAs("name");
				int age = row.getAs("age");
				Person p = new Person();
				p.setName(name);
				p.setAge(age);
				return p;
			}
		}).collect();

		for (Person p : persons) {
			System.out.println(p);
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
