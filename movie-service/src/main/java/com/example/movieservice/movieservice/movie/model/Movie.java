package com.example.movieservice.movieservice.movie.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;

import javax.persistence.Entity;
import java.io.Serializable;

//import javax.persistence.Entity;
//import javax.persistence.GeneratedValue;
//import javax.persistence.GenerationType;
//import javax.persistence.Id;

//@Entity
@ToString
@Data
@NoArgsConstructor
public class Movie implements Serializable {

    private static final long serialVersionUID = 3487495895819393L;

    @Id
    private String movieId;
    private String imdbId;
    private String tmdbId;
    private String name;

    private String url;
    private String overview;
    private String releaseDate;
    private String posterPath;
    private String homepage;
    private String video;
}
