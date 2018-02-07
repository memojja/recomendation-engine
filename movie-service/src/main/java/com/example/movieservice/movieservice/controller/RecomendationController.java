package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.model.Rating;
import com.example.movieservice.movieservice.repository.mongo.RatingRepository;
import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import scala.Tuple2;

import java.util.List;

@RestController
@RequestMapping("/recomendations")
public class RecomendationController {

    private final RatingRepository ratingRepository;

    @Autowired
    public RecomendationController(RatingRepository ratingRepository) {
        this.ratingRepository = ratingRepository;
    }




    @GetMapping("/user/{id}")
    public String getRecomendationByUserId(@PathVariable("id") Integer id){

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
        for (org.apache.spark.mllib.recommendation.Rating ratings : recommendedsFor4169) {
            System.out.println("Product id : " + ratings.product() + "-- Rating : " + ratings.rating());
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

        return "hello";
    }




















//    @GetMapping("user")
//    public String getRecomendationByUserId(){
//
//        SparkSession spark = SparkSession.builder()
//                .master("local")
//                .appName("MongoSparkConnectorIntro")
//                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
//                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
//                .getOrCreate();
//
//
//
//
//        // Create a JavaSparkContext using the SparkSession's SparkContext object
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        jsc.setLogLevel("OFF");
//
//
//        // convert pojo to json using Gson and parse using Document.parse()
//
//
//
//
//
//    /*Start Example: Read data from MongoDB************************/
//        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
//    /*End Example**************************************************/
//
//        // Analyze data from MongoDB
//
//        List<Rating> ratings1 = rdd.map(line -> {
//            Gson gson = new Gson();
//            Rating rating = gson.fromJson(line.toJson(), Rating.class);
//            System.out.println(rating);
//            Document document = Document.parse(line.toJson());
//            return rating;
//        })
//        .collect();
//
//        System.out.println(ratings1.size());
//        ratings1.stream().forEach(a -> System.out.println("===="+a));
//        System.out.println(ratings1.size());
//
////        JavaSparkContext jsc = new JavaSparkContext("local","Remondation Engine");
////        jsc.setLogLevel("OFF");
////
////        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD = getRatingJavaRDD(jsc);
//
//
//
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD = jsc.parallelize(ratings1).map(
//                        line ->{
//
//                            Integer userId = Integer.parseInt(line.getUserId());
//                            Integer movieId = Integer.parseInt(line.getMovieId());
//                            Double rating = Double.parseDouble(line.getRating());
//                            return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
//                        }).cache();
//
//
//
//
//
//
//
//        //Prediction use cases
//
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating>[] ratingSplits = ratingRDD.randomSplit(new double[] { 0.8, 0.2 });
//
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> trainingRatingRDD = ratingRDD.cache();
//
////        JavaRDD<org.apache.spark.mllib.recommendation.Rating> trainingRatingRDD = ratingSplits[0].cache();
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> testRatingRDD = ratingSplits[1].cache();
//
//        //Create prediction model (Using ALS)
//        org.apache.spark.mllib.recommendation.ALS als = new org.apache.spark.mllib.recommendation.ALS();
//        MatrixFactorizationModel model = als.setRank(10).setIterations(10).run(trainingRatingRDD);
//
//        System.out.println(trainingRatingRDD.toString());
//
//
//        // Get the top 5 movie predictions for user 4169
//        org.apache.spark.mllib.recommendation.Rating[] recommendedsFor4169 = model.recommendProducts(99999999, 5);
//        System.out.println("Recommendations for 4169");
//
//        for (org.apache.spark.mllib.recommendation.Rating ratings : recommendedsFor4169) {
//            System.out.println("Product id : " + ratings.product() + "-- Rating : " + ratings.rating());
//        }
//
//
//
//
//        // Get user product pair from testRatings
//        JavaPairRDD<Integer, Integer> testUserProductRDD = testRatingRDD.mapToPair(
//                rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
//        );
//
//
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> predictionsForTestRDD = model.predict(testUserProductRDD);
//
//        System.out.println("Test predictions");
//        predictionsForTestRDD.take(10).stream().forEach(rating -> {
//            System.out.println("Product id : " + rating.product() + "-- Rating : " + rating.rating());
//        });
//
////
////
////
//
//
//
//
//
//
//        jsc.close();
//
//        return "hello";
//    }













//    private static JavaRDD<org.apache.spark.mllib.recommendation.Rating> getRatingJavaRDD(JavaSparkContext jsc) {
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD = jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\dataset\\20m\\ratings.csv")
//                .map(
//                        line ->{
//                            String[] ratingArr = line.split("::");
//                            Integer userId = Integer.parseInt(Try.ofFailable(() -> ratingArr[0]).orElse("-1"));
//                            Integer movieId = Integer.parseInt(Try.ofFailable(() -> ratingArr[1]).orElse("-1"));
//                            Double rating = Double.parseDouble(Try.ofFailable(() -> ratingArr[2]).orElse("-1"));
//                            return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
//                        }).cache();
//
//        return ratingRDD;
//    }







}
