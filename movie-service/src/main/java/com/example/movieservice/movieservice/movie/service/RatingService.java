package com.example.movieservice.movieservice.movie.service;

import com.example.movieservice.movieservice.movie.model.Rating;

import java.util.List;

public interface RatingService {
    void save(Rating rating);
    void saveRatings(List<Rating> ratingList);
}
