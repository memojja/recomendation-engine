package com.example.movieservice.movieservice.model.movie;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class MovieDTO {

    private String budget;
    private String vote_average;
    private String backdrop_path;
    private String status;
    private String runtime;
    private String adult;
    private String homepage;
    private String id;
    private String title;
    private String original_language;
    private String overview;
    private String imdb_id;
    private String release_date;
    private String original_title;
    private String vote_count;
    private String poster_path;
    private String video;
    private String tagline;
    private String revenue;
    private String popularity;

    private Genres[] genres;
    private Spoken_languages[] spoken_languages;
    private Production_countries[] production_countries;
    private Production_companies[] production_companies;
    private Belongs_to_collection belongs_to_collection;


}
