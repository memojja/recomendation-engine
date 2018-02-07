package com.example.movieservice.movieservice.repository.mongo;

import com.example.movieservice.movieservice.model.Movie;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MovieRepository extends MongoRepository<Movie,String>,PagingAndSortingRepository<Movie,String>{
}
