package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;

/**
 * @author huangyueran
 * @category 通过反射动态转换RDD和DataFrame
 * @time 2019-7-24 13:58:59
 */
public class RDD2DataFrameReflectionDynamic {
    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(RDD2DataFrameReflectionDynamic.class);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lineRDD = sc.textFile(Constant.LOCAL_FILE_PREX + "/data/resources/people.txt");

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
        // Spark2.0之后，DataFrame和DataSet合并为更高级的DataSet，新的DataSet具有两个不同的API特性：
        // 1.非强类型(untyped)，DataSet[Row]是泛型对象的集合，它的别名是DataFrame；
        // 2.强类型(strongly-typed)，DataSet[T]是具体对象的集合，如scala和java中定义的类
        Dataset<Row> personDataset = sqlContext.createDataFrame(personsRDD, Person.class);// RDD数据,格式Schem
        personDataset.printSchema();
        // 有了DataFrame可以注册一个临时表, 使用SQL语句查询
        personDataset.registerTempTable("people");
        Dataset<Row> teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19");

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
