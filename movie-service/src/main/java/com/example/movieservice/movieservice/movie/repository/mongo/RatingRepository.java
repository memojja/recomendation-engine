package com.example.movieservice.movieservice.movie.repository.mongo;

import com.example.movieservice.movieservice.movie.model.Rating;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RatingRepository extends MongoRepository<Rating,String>{
    Rating findByUserId(String userId);
    Rating findByMovieId(String movieId);
    Rating findByRating(String ratingId);
    Rating findByUserIdAndMovieId(String userId, String movieId);
}
