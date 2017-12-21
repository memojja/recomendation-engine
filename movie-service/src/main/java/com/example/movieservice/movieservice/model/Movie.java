package com.example.movieservice.movieservice.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.io.Serializable;

@Entity
@ToString
@Data
@NoArgsConstructor
public class Movie implements Serializable {
    private static final long serialVersionUID = 3487495895819393L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY )
    private Long id;

    private String name;

    private String url;

    private String movieId;
    private String imdbId;
    private String tmdbId;






}
