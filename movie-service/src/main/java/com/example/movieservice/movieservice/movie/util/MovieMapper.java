package com.example.movieservice.movieservice.movie.util;

import com.example.movieservice.movieservice.movie.model.Movie;
import com.jasongoodwin.monads.Try;

public class MovieMapper {

    public static Movie stringToMovie(String line){
        String[] movieArr = line.split(",");
        Movie movie = new Movie();
        movie.setMovieId(movieArr[0]);
        movie.setName(movieArr[1]);
        movie.setUrl("********");
        return movie;
    }

    public static Movie mergeLinks(String line){
        String[] movieArr= line.split(",");
        Movie movie = new Movie();
        movie.setMovieId(movieArr[0]);
        movie.setTmdbId(Try.ofFailable(() -> movieArr[2]).orElse("*****"));
        movie.setImdbId(movieArr[1]);
        return movie;
    }
}
