package com.example.movieservice.movieservice.movie.controller;

import com.example.movieservice.movieservice.movie.model.Movie;
import com.example.movieservice.movieservice.movie.model.Rating;
import com.example.movieservice.movieservice.movie.service.RatingService;
import com.example.movieservice.movieservice.movie.service.RecomendationService;
import com.example.movieservice.movieservice.registration.security.CurrentUser;
import com.example.movieservice.movieservice.registration.security.UserPrincipal;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.log4j.Log4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.web.bind.annotation.*;


import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/")
@Log4j
@CrossOrigin("*")
public class RecomendationController {


    private final RecomendationService recomendationService;

    @Autowired
    public RecomendationController(RecomendationService recomendationService) {
        this.recomendationService = recomendationService;
    }

    @ApiOperation(value = "Get Recomendations", notes = "Create ratings for a user to recomentions movies")
    @ApiResponses(value = {
            @ApiResponse(code = 404, message = "Please check url")
    })
    @PostMapping("/recomendations")
    public Future<List<Movie>> handleRatings(@RequestBody List<Rating> ratings, @CurrentUser UserPrincipal currentUser) throws ExecutionException, InterruptedException {
        int userId = currentUser.getRecomendationId();
        return new AsyncResult<List<Movie>>(recomendationService.predictMovieToUserForOnlineLearning(ratings,userId).get());
    }

}
