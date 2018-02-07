package com.example.movieservice.movieservice;

import com.example.movieservice.movieservice.model.Movie;
import com.example.movieservice.movieservice.model.Rating;
import com.example.movieservice.movieservice.model.movie.MovieDTO;
import com.example.movieservice.movieservice.repository.mongo.MovieRepository;
import com.example.movieservice.movieservice.repository.mongo.RatingRepository;
import com.jasongoodwin.monads.Try;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class DatabaseLoader implements CommandLineRunner {

    private final MovieRepository movieRepository;
    private final RatingRepository ratingRepository;

    @Autowired
    public DatabaseLoader(MovieRepository movieRepository, RatingRepository ratingRepository) {
        this.movieRepository = movieRepository;
        this.ratingRepository = ratingRepository;
    }
    @Override
    public void run(String... strings) throws Exception {

    }

/*
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




        List<Movie> movieRDD = getMovieRDD(jsc);
        List<Movie> lastMovieList = getLinksRDD(jsc);
        //imdb and tmdb id
//        mergeLinkRDDAndMovieRDD(movieRDD, lastMovieList);

        RestTemplate restTemplate = new RestTemplate();
        List<Movie> movies = movieRepository.findAll();
        List<MovieDTO> movieDTOS  = new ArrayList<>();

        for (int i = 0; i < movies.size(); i++) {
            if((!movies.get(i).getTmdbId().equals("********")) && !movies.get(i).getTmdbId().equals("999999999") ) {
                ResponseEntity<MovieDTO> resultGoMonth = updateMovieRepositoryFromImdbApiForMovieImageUrl(movieRDD, restTemplate, movies, movieDTOS, i);
                downloadMovieImage(movies, i, resultGoMonth);
            }
        }

//        List<Rating> ratingRdd = getRatingRDD(jsc);
//        saveRating(ratingRdd);


    }
*/
    private void getUgurMovie() {
        Rating rating = new Rating();
        rating.setId("1000230");
        rating.setUserId("99999999");
        rating.setMovieId("99114");
        rating.setRating("5");
        ratingRepository.save(rating);

        Rating rating1 = new Rating();
        rating1.setId("1000231");
        rating1.setUserId("99999999");
        rating1.setMovieId("52319");
        rating1.setRating("5");
        ratingRepository.save(rating1);

        Rating ratin2 = new Rating();
        ratin2.setId("1000232");
        ratin2.setUserId("99999999");
        ratin2.setMovieId("6874");
        ratin2.setRating("5");
        ratingRepository.save(ratin2);

        Rating rating296  = new Rating();
        rating296.setId("1000233");
        rating296.setUserId("99999999");
        rating296.setMovieId("296");
        rating296.setRating("5");
        ratingRepository.save(rating296);


        Rating rating4419 = new Rating();
        rating4419.setId("1000234");
        rating4419.setUserId("99999999");
        rating4419.setMovieId("44191");
        rating4419.setRating("5");
        ratingRepository.save(rating4419);


        Rating rating6016 = new Rating();
        rating6016.setId("1000235");
        rating6016.setUserId("99999999");
        rating6016.setMovieId("6016");
        rating6016.setRating("5");
        ratingRepository.save(rating6016);


        Rating rating4011 = new Rating();
        rating4011.setId("1000236");
        rating4011.setUserId("99999999");
        rating4011.setMovieId("4011");
        rating4011.setRating("5");
        ratingRepository.save(rating4011);
    }

    private void saveRating(List<Rating> ratingRdd) {
        for (int i = 0; i < ratingRdd.size(); i++) {
            ratingRdd.get(i).setId(Integer.toString(i));
            System.out.println( i + "  " + ratingRdd.get(i));
            ratingRepository.save(ratingRdd.get(i));
        }
    }

    private List<Rating> getRatingRDD(JavaSparkContext jsc) {
        return jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\dataset\\20m\\ratings.csv")
                .map(line -> {
//                UserID::MovieID::Rating::Timestamp
//                    String[] ratingArr = line.split("::");
                    String[] ratingArr = line.split(",");
                    Rating rating = new Rating();
                    rating.setUserId(ratingArr[0]);
                    rating.setMovieId(ratingArr[1]);
                    rating.setRating(ratingArr[2]);
                    rating.setTimeStamp(ratingArr[3]);
                    return rating;
                }).collect();
    }

    private ResponseEntity<MovieDTO> updateMovieRepositoryFromImdbApiForMovieImageUrl(List<Movie> movieRDD, RestTemplate restTemplate, List<Movie> movies, List<MovieDTO> movieDTOS, int i) {
        String url = "https://api.themoviedb.org/3/movie/" + movies.get(i).getTmdbId() + "?api_key=a44bfe5ae077fd38027c9c495a03e853";
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        HttpEntity entity = new HttpEntity(httpHeaders);
        ResponseEntity<MovieDTO> resultGoMonth=null;
        try {
            resultGoMonth = Optional.of(restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    MovieDTO.class)).orElse(new ResponseEntity<MovieDTO>(HttpStatus.NOT_FOUND) );
            System.out.println(i + "[" + movieRDD.get(i).getTmdbId() + "]" + resultGoMonth);
            System.out.println(movies.get(i));
            movieDTOS.add(resultGoMonth.getBody());
            movies.get(i).setUrl(resultGoMonth.getBody().getPoster_path());


            movieRepository.save(movies);

            System.out.println(i + " " + resultGoMonth.getBody());
//            try {
//                Thread.sleep(50L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            return resultGoMonth;

        }
        catch (final Exception e) {
            System.out.println(e);
        }

        return new ResponseEntity<MovieDTO>(HttpStatus.OK);

    }

    private void downloadMovieImage(List<Movie> movies, int i, ResponseEntity<MovieDTO> resultGoMonth) {
        String imageUrl = "https://image.tmdb.org/t/p/w1280" +movies.get(i).getUrl();

        String destinationFilePath = "images/"+resultGoMonth.getBody().getPoster_path().substring(1,resultGoMonth.getBody().getPoster_path().length()); // For windows something like c:\\path\to\file\test.jpg

        InputStream inputStream = null;
        try {
            inputStream = new URL(imageUrl).openStream();
            Files.copy(inputStream, Paths.get(destinationFilePath));
        } catch (IOException e) {
            System.out.println("Exception Occurred " + e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    // TmdbId inject movieRDD(movie.cvs) from  lastMovieList(links.cvs)
    private void mergeLinkRDDAndMovieRDD(List<Movie> movieRDD, List<Movie> lastMovieList) {
        Boolean isActive = false;
        for (int i = 0; i <movieRDD.size() ; i++) {
            isActive=false;
            for (int a = 0; a <lastMovieList.size(); a++) {
                if(movieRDD.get(i).getMovieId().equals(lastMovieList.get(a).getMovieId()  )){
                    movieRDD.get(i).setTmdbId(lastMovieList.get(a).getTmdbId());
                    movieRDD.get(i).setImdbId(lastMovieList.get(a).getImdbId());
                    System.out.println("[[[[[["+movieRDD.get(i).getTmdbId());
                    isActive=true;
                }
            }
            if (!isActive){
                movieRDD.get(i).setTmdbId("********");
                movieRDD.get(i).setImdbId("********");
                movieRDD.get(i).setUrl("********");
            }
    //           if save it make comment out
               movieRepository.save(movieRDD.get(i));
        }
    }

    private List<Movie> getLinksRDD(JavaSparkContext jsc) {
        return jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\dataset\\20m\\links.csv")
                .map(line ->{
                            String[] movieArr= line.split(",");
                            Movie movie = new Movie();
                            movie.setMovieId(movieArr[0]);
                            movie.setTmdbId(Try.ofFailable(() -> movieArr[2]).orElse("*****"));

                            movie.setImdbId(movieArr[1]);
                            return movie;
                        }
                )
                .collect();
    }

    private List<Movie> getMovieRDD(JavaSparkContext jsc) {
        return jsc.textFile("C:\\Users\\memojja\\Desktop\\BITIRME TEZI\\movie-service\\src\\main\\resources\\dataset\\20m\\movies.csv")
                .map(line -> {
//                    String[] movieArr = line.split("::");
                    String[] movieArr = line.split(",");
                    Movie movie = new Movie();
//                    System.out.println(movieArr[2]);
                    movie.setMovieId(movieArr[0]);
                    movie.setName(movieArr[1]);
                    movie.setUrl("********");
                    return movie;
                }).collect();
    }


}
