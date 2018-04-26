package com.example.movieservice.movieservice.movie.util;

import com.example.movieservice.movieservice.movie.model.Rating;

public class RatingMapper {

    public static org.apache.spark.mllib.recommendation.Rating ratingToMLRating(Rating rating){
        Integer userId = Integer.parseInt(rating.getUserId());
        Integer movieId = Integer.parseInt(rating.getMovieId());
        Double ratingValue = Double.parseDouble(rating.getRating());
        return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingValue);
    }

    public static org.apache.spark.mllib.recommendation.Rating ratingToMLRatingwithUserId(Rating rating,int user){
        Integer userId = user;
        Integer movieId = Integer.parseInt(rating.getMovieId());
        Double ratingValue = Double.parseDouble(rating.getRating());
        return new org.apache.spark.mllib.recommendation.Rating(userId, movieId, ratingValue);
    }

    public static Rating lineToRating(String line){
        String[] ratingArr = line.split(",");
        return new Rating(ratingArr[0],ratingArr[1],ratingArr[2]);
    }

}
