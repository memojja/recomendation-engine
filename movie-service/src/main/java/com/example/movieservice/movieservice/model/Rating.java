package com.example.movieservice.movieservice.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;

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
//    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
//    private Long id;
    @Id
    private String id;
    private String userId;
    private String movieId;
    private String rating;
    private String timeStamp;
}
