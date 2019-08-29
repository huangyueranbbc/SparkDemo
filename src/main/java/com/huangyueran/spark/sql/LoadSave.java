package com.huangyueran.spark.sql;

import com.huangyueran.spark.utils.Constant;
import com.huangyueran.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

/**
 * @author huangyueran
 * @category LoadSave
 * @time 2019-7-24 13:58:59
 */
public class LoadSave {
    public static void main(String[] args) {
        JavaSparkContext sc = SparkUtils.getRemoteSparkContext(LoadSave.class);

        // 创建DataFrame 读取json
        SQLContext sqlContext = new SQLContext(sc);

        DataFrameReader dataFrameReader = sqlContext.read();

        // parquet 是本地数据存储的格式
        Dataset<Row> dataset = dataFrameReader.load("/data/resources/users.parquet");

        dataset.printSchema();
        dataset.show();

        // 通过关writer写入并保存save
        DataFrameWriter write = dataset.select("name", "favorite_color").write();
        write.save("tmp/namesAndColors.parquet");

        Dataset<Row> load = dataFrameReader.load("tmp/namesAndColors.parquet/part-00000-c8ac19a7-9e13-49e2-a4d0-7cfe469bdfcf-c000.snappy.parquet");
        load.show();

        sc.close();
    }
}
