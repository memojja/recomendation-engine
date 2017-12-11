package com.example.movieservice.movieservice.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MovieController {

    @GetMapping(value = {"/","/home","/index"})
    public String index(){
        return "Hello World!";
    }
}
