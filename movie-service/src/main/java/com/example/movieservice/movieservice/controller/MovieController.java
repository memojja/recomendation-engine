package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.model.movie.MovieDTO;
import com.example.movieservice.movieservice.repository.MovieRepository;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.io.*;
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

    @GetMapping(value = "/{id}/image",produces = MediaType.IMAGE_JPEG_VALUE )
    @ResponseBody
    public ResponseEntity<byte[]> getMovieImageById(@PathVariable("id")Long id) throws IOException {
        String filename="images"+movieRepository.findOne(id).getUrl();
        InputStream inputImage = new FileInputStream(filename);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[512];
        int l = inputImage.read(buffer);
        while(l >= 0) {
            outputStream.write(buffer, 0, l);
            l = inputImage.read(buffer);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "image/png");
        return new ResponseEntity<byte[]>(outputStream.toByteArray(), headers, HttpStatus.OK);
    }
}
