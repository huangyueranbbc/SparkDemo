package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

/**
 * @author huangyueran
 * @category format()简单格式的格式转换存储
 * @time 2019-7-24 14:33:23
 */
public class SpecifyFormatLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SpecifyFormatLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建DataFrame 读取json
        SQLContext sqlContext = new SQLContext(sc);

        DataFrameReader dataFrameReader = sqlContext.read();

        // parquet 是本地数据存储的格式
        Dataset<Row> dataset = dataFrameReader.format("json").load(Constant.LOCAL_FILE_PREX + "/data/resources/people.json");

        // 通过关writer写入并保存save
        DataFrameWriter write = dataset.select("name").write();
        write.format("parquet").save("tmp/people.parquet");

        sc.close();
    }
}
