package com.example.movieservice.movieservice.advice;

import com.example.movieservice.movieservice.movie.exception.ImageNotReadException;
import com.example.movieservice.movieservice.movie.exception.MovieNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Collections;


@RestControllerAdvice
public class ExceptionHandlerAdvice {

    @ExceptionHandler(MovieNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Object handleNotFound(MovieNotFoundException notFound){
        return Collections.singletonMap("message", notFound.getMessage());
    }

    @ExceptionHandler(ImageNotReadException.class)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Object handleImageNotRead(ImageNotReadException notFound){
        return Collections.singletonMap("message", notFound.getMessage());
    }


}
