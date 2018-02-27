package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.UserProductTuple;
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
import org.apache.spark.api.java.*;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import scala.Tuple2;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//import org.apache.spark.mllib.recommendation.ALS;
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

                List<Movie> recoemndations = new ArrayList<>();




        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")

//                .config("spark.executor.memory", "10g")
//                .config("spark.network.timeout", "60000s")


                .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "100")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.partitionKey","70")



        .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("OFF");

//
//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD  = rdd.map( line ->{
//            Gson gson = new Gson();
//            Rating ratt = gson.fromJson(line.toJson(), Rating.class);
//            Integer userId = Integer.parseInt(ratt.getUserId());
//            Integer movieId = Integer.parseInt(ratt.getMovieId());
//            Double rating = Double.parseDouble(ratt.getRating());
//            System.out.println("***"+ratt);
//            return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
//        }).cache();

        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);


        System.out.println("memojjjaaaaa"+rdd.partitions().size());



        List<Rating> ratings1 = rdd.map(line -> {
            Gson gson = new Gson();
            return gson.fromJson(line.toJson(), Rating.class);
        }).collect();





        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ratingRDD = jsc
                .parallelize(ratings1).map(
                ratt ->{
                    Integer userId = Integer.parseInt(ratt.getUserId());
                    Integer movieId = Integer.parseInt(ratt.getMovieId());
                    Double rating = Double.parseDouble(ratt.getRating());
                    System.out.println("----"+ratt);
                    return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
        }).cache();





        JavaRDD<org.apache.spark.mllib.recommendation.Rating>[] ratingSplits =
                ratingRDD.randomSplit(new double[] { 0.8, 0.2 });

        JavaRDD<org.apache.spark.mllib.recommendation.Rating> trainingRatingRDD = ratingRDD;

//        JavaRDD<org.apache.spark.mllib.recommendation.Rating> trainingRatingRDD = ratingSplits[0].cache();
        JavaRDD<org.apache.spark.mllib.recommendation.Rating> testRatingRDD = ratingSplits[1].cache();



        //Create prediction model (Using ALS)
        org.apache.spark.mllib.recommendation.ALS als = new org.apache.spark.mllib.recommendation.ALS();

        MatrixFactorizationModel model = als
                .setRank(10)
                .setIterations(10)
                .setImplicitPrefs(true)
                .run(trainingRatingRDD);

        org.apache.spark.mllib.recommendation.Rating[]
                recommendedsFor4169 = model.recommendProducts(99999999, 5);


        System.out.println("Recommendations for 4169");
        Movie movie;
        for (org.apache.spark.mllib.recommendation.Rating ratings : recommendedsFor4169) {
            System.out.println("Product id : " + ratings.product() + "-- Rating : " + ratings.rating());
            movie = movieRepository.findOne(String.valueOf(ratings.product()));
            recoemndations.add(movie);
        }


        System.out.println(recoemndations);




        // Get user product pair from testRatings
        JavaPairRDD<Integer, Integer> testUserProductRDD = testRatingRDD.mapToPair(
                rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
        );

        JavaRDD<org.apache.spark.mllib.recommendation.Rating> predictionsForTestRDD = model.predict(testUserProductRDD);




        System.out.println("Test predictions");
        predictionsForTestRDD.take(10).stream().forEach(rating -> {
            System.out.println("Product id : " + rating.product() + "-- Rating : " + rating.rating());
        });

//We will compare the test predictions to the actual test ratings


        JavaPairRDD<UserProductTuple, Double> predictionsKeyedByUserProductRDD = predictionsForTestRDD.mapToPair(
                rating -> new Tuple2<>(new UserProductTuple(rating.user(), rating.product()), rating.rating())
        );


        JavaPairRDD<UserProductTuple, Double> testKeyedByUserProductRDD = testRatingRDD.mapToPair(
                rating -> new Tuple2<UserProductTuple, Double>(new UserProductTuple(rating.user(), rating.product()),rating.rating())
        );







        JavaPairRDD<UserProductTuple, Tuple2<Double,Double>> testAndPredictionsJoinedRDD  = testKeyedByUserProductRDD.join(predictionsKeyedByUserProductRDD);

        System.out.println("**************************");
        testAndPredictionsJoinedRDD.take(10).forEach(k ->{
            System.out.println(
                    "UserID : " + k._1.getUserId() +
                            "||ProductId: " + k._1.getProductId() +
                            "|| Test Rating : " + k._2._1 +
                            "|| Predicted Rating : " + k._2._2);
        });
        System.out.println("**************************");


//		 Find false positives

        JavaPairRDD<UserProductTuple, Tuple2<Double,Double>> falsePositives =  testAndPredictionsJoinedRDD.filter(
                k -> k._2._1 <= 1 && k._2._2 >=4
        );

        System.out.println("testAndPredictionsJoinedRDD  count : " + testAndPredictionsJoinedRDD.count());
        System.out.println("False positives count : " + falsePositives.count());


// Find absolute differences between the predicted and actual targets.

        JavaDoubleRDD meanAbsoluteError = testAndPredictionsJoinedRDD.mapToDouble(
                v1 -> Math.abs(v1._2._1 - v1._2._2));

        System.out.println("Mean : " + meanAbsoluteError.mean());








        System.out.println("i gues..it will be that");






        JavaRDD<org.apache.spark.mllib.recommendation.Rating> ugurRatingRDD = jsc
                .parallelize(ratings1).map(
                        ratt ->{
                            Integer userId = 5;
                            Integer movieId = Integer.parseInt(ratt.getMovieId());
                            Double rating = Double.parseDouble(ratt.getRating());
                            System.out.println("----"+ratt);
                            return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, rating);
                        }).cache();

        //customize get ugurs movies offline mode ----------------------------------
        List<org.apache.spark.mllib.recommendation.Rating> ugurRatings =getUgurMovie();



        // Get user product pair from testRatings
        JavaPairRDD<Integer, Integer> testUserProductRDD1 = jsc.parallelize(ugurRatings).mapToPair(
                rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
        );


        JavaRDD<org.apache.spark.mllib.recommendation.Rating> predictionsForTestRDD1 =
                model.predict(testUserProductRDD1);


        System.out.println("ugurun sonuclari");
        predictionsForTestRDD1.take(10).stream().forEach(rating -> {
            System.out.println(" ddd  Product id : " + rating.product() + "-- Rating : " + rating.rating());
        });
        //customize get ugurs movies offline mode ----------------------------------





        model.save(jsc.sc(),"C:/Users/memojja/Desktop/tezZ/my-graduation-thesis/movie-service/modell");


        jsc.close();
//
//        https://stackoverflow.com/questions/41134929/how-to-to-retrieve-the-saved-als-model-using-spark-java
//        ******  SONUCLARI BIRYERE KAYDET ORADAN OKU   ***********

//        MatrixFactorizationModel model;
//        MatrixFactorizationModel sameModel;
//
//        int rank = 10;
//        int numIterations = 10;
//        model = ALS.train( JavaRDD.toRDD( ratings ), rank, numIterations, 0.01 );
//        model.save( sc.sc(), "src/main/resources/UserBasedModel" );

//        sameModel = MatrixFactorizationModel.load( sc.sc(), "src/main/resources/UserBasedModel" );

        return recoemndations;
    }


    @GetMapping("/hiii")
    public String sad(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/movie-service-db.rating")

//                .config("spark.executor.memory", "10g")
//                .config("spark.network.timeout", "60000s")


                .config("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.numberOfPartitions", "100")
                .config("spark.mongodb.input.partitionerOptions.MongoPaginateByCountPartitioner.partitionKey","70")



                .getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");

        try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
            jsc.setLogLevel("OFF");
            MatrixFactorizationModel model =
                    MatrixFactorizationModel.load(jsc.sc(), "C:/Users/memojja/Desktop/tezZ/my-graduation-thesis/movie-service/modell");
            List<org.apache.spark.mllib.recommendation.Rating> ugurRatings =getUgurMovie();

            JavaPairRDD<Integer, Integer> testUserProductRDD1 = jsc.parallelize(ugurRatings).mapToPair(
                    rating -> new Tuple2<Integer, Integer>(rating.user(), rating.product())
            );


            JavaRDD<org.apache.spark.mllib.recommendation.Rating> predictionsForTestRDD1 =
                    model.predict(testUserProductRDD1);



            System.out.println("ugurun sonuclari");
            predictionsForTestRDD1.take(10).stream().forEach(rating -> {
                System.out.println(" ddd  Product id : " + rating.product() + "-- Rating : " + rating.rating());
            });


        }


        return "asdsad";

    }

    private  List<org.apache.spark.mllib.recommendation.Rating> getUgurMovie() {

        List<org.apache.spark.mllib.recommendation.Rating> ugurMovies =
                new ArrayList<>();

        Integer userId = 5;
        Double ratingg = 5.0;

        Integer movieId = 99114;
        org.apache.spark.mllib.recommendation.Rating rating =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(rating);



         movieId = 6016;
        org.apache.spark.mllib.recommendation.Rating w =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(w);


         movieId = 4011;
        org.apache.spark.mllib.recommendation.Rating s =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(s);


         movieId = 44191;
        org.apache.spark.mllib.recommendation.Rating d =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(d);


         movieId = 296;
        org.apache.spark.mllib.recommendation.Rating c =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(c);


         movieId = 6874;
        org.apache.spark.mllib.recommendation.Rating b =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(b);


         movieId = 52319;
        org.apache.spark.mllib.recommendation.Rating a =
                new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingg);
        ugurMovies.add(a);



        return ugurMovies;

    }

}
