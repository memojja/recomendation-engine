package com.example.movieservice.movieservice.movie.service.impl;

import com.example.movieservice.movieservice.movie.util.RatingMapper;
import com.example.movieservice.movieservice.movie.model.Movie;
import com.example.movieservice.movieservice.movie.model.Rating;
import com.example.movieservice.movieservice.movie.repository.mongo.MovieRepository;
import com.example.movieservice.movieservice.movie.service.RecomendationService;
import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Log4j
public class RecomendationServiceImpl implements RecomendationService {

    private final MovieRepository movieRepository;
    private final JavaSparkContext jsc;

    @Autowired
    public RecomendationServiceImpl(MovieRepository movieRepository, JavaSparkContext jsc) {
        this.movieRepository = movieRepository;
        this.jsc = jsc;
    }

    @Override
    @Async
    public CompletableFuture<List<Movie>> predictMovieToUserForOnlineLearning(List<Rating> incomeRatings, int userId){

        org.apache.spark.mllib.recommendation.Rating[] recomendationModel
                = getRecomendationMovies(mlibRatings(incomeRatings,userId), userId);

        log.info("Recommendations for" + userId);
        List<Movie> recomendations = Arrays.stream(recomendationModel).map(
                movie ->  {
                    log.info("Product id : " + movie.product() + "-- Rating : " + movie.rating());
                    return movieRepository.findOne(String.valueOf(movie.product()));
                }
              ).collect(Collectors.toList());

        return CompletableFuture.completedFuture(recomendations);
    }

    private  List<org.apache.spark.mllib.recommendation.Rating> mlibRatings(List<Rating> ratings,Integer userId) {
//        ratingService.saveRatings(ratings);
        return ratings.stream()
                .map(rating ->  RatingMapper.ratingToMLRatingwithUserId(rating,userId))
                .collect(Collectors.toList());
    }

    private org.apache.spark.mllib.recommendation.Rating[] getRecomendationMovies(List<org.apache.spark.mllib.recommendation.Rating> incomeRatings, Integer userId) {
        MatrixFactorizationModel model = getMatrixFactorizationModel(incomeRatings);
        return model.recommendProducts(userId, 5);
    }

    private MatrixFactorizationModel getMatrixFactorizationModel(List<org.apache.spark.mllib.recommendation.Rating> incomeRatings) {
        org.apache.spark.mllib.recommendation.ALS als = new org.apache.spark.mllib.recommendation.ALS();
        return als
                .setRank(10)
                .setIterations(10)
                .setImplicitPrefs(true)
                .run(getJavaRDD().union(jsc.parallelize(incomeRatings)));
    }

    private JavaRDD<org.apache.spark.mllib.recommendation.Rating> getJavaRDD() {
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        List<Rating> ratings1 = rdd.map(line -> {
            Gson gson = new Gson();
            return gson.fromJson(line.toJson(), Rating.class);
        }).collect();

        return jsc.parallelize(ratings1).map(RatingMapper::ratingToMLRating).cache();
    }
}
