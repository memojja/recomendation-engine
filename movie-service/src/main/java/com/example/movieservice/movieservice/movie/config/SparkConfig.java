package com.example.movieservice.movieservice.movie.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class SparkConfig {

    @Bean
    public JavaSparkContext getJavaSparkContext(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
//        spark.sql.thriftServer.incrementalCollect=true
                .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "100")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.partitionKey","70")
//                .config("spark.executor.memory", "10g")
//                .config("spark.network.timeout", "60000s")
                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("OFF");

        return jsc;
    }

}
