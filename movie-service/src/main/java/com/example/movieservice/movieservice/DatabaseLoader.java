package com.example.movieservice.movieservice;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.model.movie.MovieDTO;
import com.example.movieservice.movieservice.repository.MovieRepository;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class DatabaseLoader implements CommandLineRunner {


    private final MovieRepository movieRepository;


    @Autowired
    public DatabaseLoader(MovieRepository movieRepository) {
        this.movieRepository = movieRepository;
    }

    @Override
    public void run(String... strings) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("Spark MultipleContest Test");
        conf.set("spark.driver.allowMultipleContexts", "true");
        conf.setMaster("local");
        conf.getAll();

        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        JavaSparkContext jsc =JavaSparkContext.fromSparkContext(sc);



        List<Movie> movieRDD = jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\dataset\\movies.dat")
                .map(line -> {
                    String[] movieArr = line.split("::");
                    Movie movie = new Movie();
                    System.out.println(movieArr[2]);
                    movie.setMovieId(movieArr[0]);
                    movie.setName(movieArr[1]);
                    return movie;
                }).collect();


        List<Movie> lastMovieList = jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\links.csv")
                .map(line ->{
                            String[] movieArr= line.split(",");
                            Movie movie = new Movie();
                            movie.setMovieId(movieArr[0]);
                            movie.setTmdbId(movieArr[2] == ""  ?  "*****" : movieArr[2]);
                            movie.setImdbId(movieArr[1]);
                            return movie;
                        }
                )
                .collect();

// TmdbId inject movieRDD(movie.cvs) from  lastMovieList(links.cvs)
        Boolean isActive = false;
        for (int i = 0; i <movieRDD.size() ; i++) {
            isActive=false;
            for (int a = 0; a <lastMovieList.size(); a++) {
                if(movieRDD.get(i).getMovieId().equals(lastMovieList.get(a).getMovieId()  )){
                    movieRDD.get(i).setTmdbId(lastMovieList.get(a).getTmdbId());
                    System.out.println("[[[[[["+movieRDD.get(i).getTmdbId());
                    isActive=true;
                }
            }
            if (!isActive)  movieRDD.get(i).setTmdbId("********");
    //           if save it make comment out
    //           movieRepository.save(movieRDD.get(i));
        }


// request themoviedb.
        String url;
        RestTemplate restTemplate = new RestTemplate();
        List<Movie> movies = movieRepository.findAll();
        List<MovieDTO> movieDTOS  = new ArrayList<>();

        for (int i = 0; i < movies.size(); i++) {
            if((!movies.get(i).getTmdbId().equals("********"))) {
                url = "https://api.themoviedb.org/3/movie/" + movies.get(i).getTmdbId() + "?api_key=a44bfe5ae077fd38027c9c495a03e853";
                final HttpHeaders httpHeaders = new HttpHeaders();
                httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
                HttpEntity entity = new HttpEntity(httpHeaders);
                ResponseEntity<MovieDTO> resultGoMonth = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        entity,
                        MovieDTO.class);
                System.out.println(i + "[" + movieRDD.get(i).getTmdbId() + "]" + resultGoMonth);
                movieDTOS.add(resultGoMonth.getBody());
                System.out.println(i + " " + resultGoMonth.getBody());

                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }




    }

}