package com.example.movieservice.movieservice.movie.repository.mongo;

import com.example.movieservice.movieservice.movie.model.Movie;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface MovieRepository extends
        MongoRepository<Movie,String>,
        PagingAndSortingRepository<Movie,String>{
//    @Cacheable("movies")
    List<Movie> findAll();
}
