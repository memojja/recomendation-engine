package com.example.movieservice.movieservice.controller;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.repository.mongo.MovieRepository;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.Produces;
import java.io.*;
import java.util.List;

@CrossOrigin("*")
@RestController
@RequestMapping(value = "/movies")
@Log4j
@Api(
        basePath = "/movies",
        produces = "application/json",
        value = "Movie",
        description = "Operations with movies")
public class MovieController {

    private final MovieRepository movieRepository;

    @Autowired
    public MovieController(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @ApiOperation(value = "Get Movies", notes = "Fetch List of Movies")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url"),
            @ApiResponse(code = 200, message = "List<Movie>"),
            @ApiResponse(code = 500, message = "Error occurred while fetching Movies")
    })
    @GetMapping
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public List<Movie> getAllMovies(){
        log.info("MovieController - getAllMovies() is called");
        return movieRepository.findAll();
    }

    @ApiOperation(value = "Get Movies", notes = "Fetch List of Movies with pageable")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url"),
            @ApiResponse(code = 200, message = "List<Movie>"),
            @ApiResponse(code = 500, message = "Error occurred while fetching Movies")
    })
    @GetMapping("/paging")
    @Produces(MediaType.APPLICATION_JSON_VALUE)
    public Page<Movie> getAllMoviesWithPageable(Pageable pageable){
        log.info("MovieController - getAllMoviesWithPageable() is called");
        return movieRepository.findAll(pageable);
    }

    @ApiOperation(value = "Get Movie", notes = "Fetch Movie")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url"),
            @ApiResponse(code = 200, message = "Movie"),
            @ApiResponse(code = 500, message = "Error occurred while fetching Movie")
    })
    @GetMapping("/{id}")
    public Movie getMovieById(@PathVariable("id")String id){
        log.info("MovieController - getMovieById() is called");
        return movieRepository.findOne(id);
    }

    @ApiOperation(value = "Get Movie's image", notes = "Fetch Movie's image")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url"),
            @ApiResponse(code = 200, message = "Movie's image"),
            @ApiResponse(code = 500, message = "Error occurred while fetching Movie")
    })
    @GetMapping(value = "/{id}/image",produces = MediaType.IMAGE_JPEG_VALUE )
    @ResponseBody
    public ResponseEntity<byte[]> getMovieImageById(@PathVariable("id")String id) throws IOException {
        log.info("MovieController - getMovieImageById() is called");

        String filename="images"+movieRepository.findOne(id).getUrl();
        ByteArrayOutputStream outputStream;
        HttpHeaders headers;
        try (InputStream inputImage = new FileInputStream(filename)) {
            outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[512];
            int l = inputImage.read(buffer);
            while (l >= 0) {
                outputStream.write(buffer, 0, l);
                l = inputImage.read(buffer);
            }
            headers = new HttpHeaders();
            headers.set("Content-Type", "image/png");
        }
        return new ResponseEntity<>(outputStream.toByteArray(), headers, HttpStatus.OK);
    }
}
