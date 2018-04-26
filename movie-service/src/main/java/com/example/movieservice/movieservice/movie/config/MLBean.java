package com.example.movieservice.movieservice.movie.config;

import com.example.movieservice.movieservice.JavaRecommendationExample;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

//@Component
public class MLBean {

//    @Autowired
    private JavaSparkContext javaSparkContext;

//    @Bean
    public MatrixFactorizationModel getMLModel(){
        return MatrixFactorizationModel
                .load(javaSparkContext.sc(), "modell");
    }
}
