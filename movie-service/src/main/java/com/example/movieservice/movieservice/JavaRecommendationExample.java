package com.example.movieservice.movieservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;
// $example off$

@Component
public class JavaRecommendationExample {

    @Autowired
    private JavaSparkContext jsc;


    public void a() {


        // Load and parse the data
        String path = "C:\\Users\\memojja\\Desktop\\tezZ\\my-graduation-thesis\\movie-service\\src\\main\\resources\\dataset\\ratings.dat";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Rating> ratings = data.map(s -> {
            String[] sarray = s.split("::");
            return new Rating(Integer.parseInt(sarray[0]),
                    Integer.parseInt(sarray[1]),
                    Double.parseDouble(sarray[2]));
        });

        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
//        MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(ratings), rank, numIterations);

        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts =
                ratings.map(r -> new Tuple2<>(r.user(), r.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();
        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();
        System.out.println("Mean Squared Error = " + MSE);

        // Save and load model
        model.save(jsc.sc(), "C:\\Users\\memojja\\Desktop\\tezZ\\my-graduation-thesis\\movie-service\\modell2");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "C:\\Users\\memojja\\Desktop\\tezZ\\my-graduation-thesis\\movie-service\\modell2");
        // $example off$
//
//        jsc.stop();
    }
}