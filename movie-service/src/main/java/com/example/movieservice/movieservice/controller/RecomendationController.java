package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.model.Rating;
import com.example.movieservice.movieservice.repository.mongo.MovieRepository;
import com.example.movieservice.movieservice.repository.mongo.RatingRepository;
import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/")
@Log4j
public class RecomendationController {

    private final RatingRepository ratingRepository;
    private final MovieRepository movieRepository;

    @Autowired
    public RecomendationController(RatingRepository ratingRepository, MovieRepository movieRepository) {
        this.ratingRepository = ratingRepository;
        this.movieRepository = movieRepository;
    }

    @ApiOperation(value = "Post Ratings", notes = "Create ratings for a user to recomentions movies")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url")
    })
    @PostMapping("")
    public void handleRatings(@RequestBody List<Rating> ratings){
        log.info("RecomendationController - handleRatings() is called. ");
        ratings.forEach(System.out::println);
    }

    @GetMapping("/user/{id}")
    public List<Movie> getRecomendationByUserId(@PathVariable("id") Integer id){

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
                .getOrCreate();


        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("OFF");



        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        List<Rating> ratings1 = rdd.map(line -> {
            Gson gson = new Gson();
            Rating rating = gson.fromJson(line.toJson(), Rating.class);
            System.out.println(rating);
            Document document = Document.parse(line.toJson());
            return rating;
        }).collect();



        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD = jsc.parallelize(ratings1).map(
                line ->{

                    Integer userId = Integer.parseInt(line.getUserId());
                    Integer movieId = Integer.parseInt(line.getMovieId());
                    Double rating = Double.parseDouble(line.getRating());
                    return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
                }).cache();




        JavaRDD<org.apache.spark.mllib.recommendation.Rating>[] ratingSplits = ratingRDD.randomSplit(new double[] { 0.8, 0.2 });

        JavaRDD<org.apache.spark.mllib.recommendation.Rating> trainingRatingRDD = ratingRDD.cache();
        JavaRDD<org.apache.spark.mllib.recommendation.Rating> testRatingRDD = ratingSplits[1].cache();

        //Create prediction model (Using ALS)
        org.apache.spark.mllib.recommendation.ALS als = new org.apache.spark.mllib.recommendation.ALS();
        MatrixFactorizationModel model = als.setRank(10).setIterations(10).run(trainingRatingRDD);


//        org.apache.spark.mllib.recommendation.Rating[] recommendedsFor4169 = model.recommendProducts(id, 5);
        org.apache.spark.mllib.recommendation.Rating[] recommendedsFor4169 = model.recommendProducts(99999999, 5);

        System.out.println("Recommendations for 4169");
        List<Movie> recoemndations = new ArrayList<>();
        Movie movie;
        for (org.apache.spark.mllib.recommendation.Rating ratings : recommendedsFor4169) {
            System.out.println("Product id : " + ratings.product() + "-- Rating : " + ratings.rating());
            movie = movieRepository.findOne(String.valueOf(ratings.product()));
            recoemndations.add(movie);
        }




        // Get user product pair from testRatings
        JavaPairRDD<Integer, Integer> testUserProductRDD = testRatingRDD.mapToPair(
                rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
        );


        JavaRDD<org.apache.spark.mllib.recommendation.Rating> predictionsForTestRDD = model.predict(testUserProductRDD);

        System.out.println("Test predictions");
        predictionsForTestRDD.take(10).stream().forEach(rating -> {
            System.out.println("Product id : " + rating.product() + "-- Rating : " + rating.rating());
        });



        jsc.close();

        return recoemndations;
    }

}
