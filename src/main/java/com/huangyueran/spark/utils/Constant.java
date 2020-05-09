package com.huangyueran.spark.utils;

import java.io.File;

/*******************************************************************************
 * @date 2019-08-09 15:30
 * @author: <a href=mailto:huangyr>黄跃然</a>
 * @Description: 常量
 ******************************************************************************/
public class Constant {

    public static final String LOCAL_FILE_PREX = "file:///" + new File(".").getAbsolutePath();

    public static final String SPARK_REMOTE_SERVER_ADDRESS = "spark://192.168.1.128:7077";

}
