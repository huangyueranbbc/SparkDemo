# SparkDEMO  [![Travis](https://img.shields.io/badge/SparkDemo-v1.0-yellowgreen.svg)](https://github.com/huangyueranbbc/SparkDemo)  [![Travis](https://img.shields.io/badge/Spark-API-green.svg)](http://spark.apache.org/docs/latest/api.html)  [![Travis](https://img.shields.io/badge/Apache-Spark-yellowgreen.svg)](http://spark.apache.org/)  [![Travis](https://img.shields.io/badge/Spark-ALS-blue.svg)](https://github.com/huangyueranbbc/Spark_ALS)  
1. 包含Spark所有的操作   
   a.包含官方的ml、mllib、streaming、sql等操作DEMO   
   b.包含所有常用算子的操作DEMO
      
2. 已修正为maven版本
   
3. 有详细的中英注释

4. spark-api版本更新至新版

5. 增加scala-spark     

2019年08月06日 
  
Spark operation DEMO  
1. Include all Spark operations
    A. Contains official operations such as ml, mllib, streaming, sql, etc. DEMO
    B. Operation DEMO containing all common operators

2. Modified to Maven version

3. Detailed Chinese and English annotations

4. Spark-api version updated to new version

5. scala-spark

2019-08-06  

## 远程执行模式
1. 根据需要，将/data目录下的文件上传到hdfs相同的目录下  
------data  
------------mllib  
------------resources  

2. mvn package生成jar包。指定jar包文件地址。  
        conf.setJars(Array[String]("/Users/huangyueran/ideaworkspaces1/myworkspaces/spark/SparkDemo/target/SparkDemo-1.0-SNAPSHOT-jar-with-dependencies.jar"))  
        
3. 通过SparkUtils选择运行模式  
        JavaSparkContext sc = SparkUtils.getLocalSparkContext(TestStorageLevel.class);  
        JavaSparkContext sc = SparkUtils.getRemoteSparkContext(TestStorageLevel.class);  
4. 使用远程模式,添加集群配置文件到resources目录下  
        core-site.xml  
        hdfs-site.xml  
        yarn-site.xml  
        
5. 如果需要加载文件，根据运行模式选择文件加载方式。  
        JavaRDD<String> text = sc.textFile(Constant.LOCAL_FILE_PREX +"/data/resources/test.txt");  
        JavaRDD<String> text = sc.textFile(/data/resources/test.txt");  
        
           
           
        

