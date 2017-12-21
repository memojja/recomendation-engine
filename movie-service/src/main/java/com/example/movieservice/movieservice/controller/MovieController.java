package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.model.movie.MovieDTO;
import com.example.movieservice.movieservice.repository.MovieRepository;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping(value = "/movies")
public class MovieController implements Serializable {

    private final MovieRepository movieRepository;

    @Autowired
    public MovieController(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @GetMapping
    public List<Movie> getAllMovies(){
        return movieRepository.findAll();
    }

    @GetMapping("/{id}")
    public Movie getMovieById(@PathVariable("id")Long id){
        return movieRepository.findOne(id);
    }

    @GetMapping(value = {"/","/home","/index"})
    public String index(){


        return "Hello World!";
    }

}
