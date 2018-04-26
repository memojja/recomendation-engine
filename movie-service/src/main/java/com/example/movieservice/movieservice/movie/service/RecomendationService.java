package com.example.movieservice.movieservice.movie.service;

import com.example.movieservice.movieservice.movie.model.Movie;
import com.example.movieservice.movieservice.movie.model.Rating;

import java.util.List;
import java.util.concurrent.CompletableFuture;


public interface RecomendationService {
//    List<Movie> predictMovieToUserForOnlineLearning(List<Rating> ratings,Integer userId);
    CompletableFuture<List<Movie>> predictMovieToUserForOnlineLearning(List<Rating> ratings, int userId);
}
