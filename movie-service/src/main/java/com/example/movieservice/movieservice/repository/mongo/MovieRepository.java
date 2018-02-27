package com.example.movieservice.movieservice.repository.mongo;

import com.example.movieservice.movieservice.model.Movie;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MovieRepository extends MongoRepository<Movie,String>,PagingAndSortingRepository<Movie,String>{

//    @Cacheable("movies")
    List<Movie> findAll();
}
