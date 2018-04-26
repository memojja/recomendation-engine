package com.example.movieservice.movieservice.movie.model;

import lombok.AllArgsConstructor;
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
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Rating implements Serializable{
    @Id
    private String id;
    private String userId;
    private String movieId;
    private String rating;
    private String timeStamp;



    public Rating(String userId, String movieId, String rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timeStamp = timeStamp;
    }

    public Rating(String id, String userId, String movieId, String rating) {
        this.id = id;
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timeStamp = timeStamp;
    }
}
