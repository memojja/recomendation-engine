package com.example.movieservice.movieservice.initializer;

import com.example.movieservice.movieservice.movie.exception.ImageNotReadException;
import com.example.movieservice.movieservice.movie.util.AppConstant;
import com.example.movieservice.movieservice.movie.util.DatasetConstant;
import com.example.movieservice.movieservice.movie.util.MovieMapper;
import com.example.movieservice.movieservice.movie.model.Movie;
import com.example.movieservice.movieservice.movie.model.Rating;
import com.example.movieservice.movieservice.movie.model.movie.MovieDTO;
import com.example.movieservice.movieservice.movie.repository.mongo.MovieRepository;
import com.example.movieservice.movieservice.movie.repository.mongo.RatingRepository;
import com.example.movieservice.movieservice.movie.util.RatingMapper;
import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import scala.App;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
@Log4j
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

//        List<Movie> movieRDD = getMovieRDD(jsc);
//        List<Movie> lastMovieList = getLinksRDD(jsc);
//
//        mergeLinkRDDAndMovieRDD(movieRDD, lastMovieList);

//        RestTemplate restTemplate = new RestTemplate();
//        List<Movie> movies = movieRepository.findAll();
//        List<MovieDTO> movieDTOS  = new ArrayList<>();

//        updateMovieFromApi(restTemplate, movies, movieDTOS);

//        downloadMovieImageAndUpdateMovie(restTemplate, movies, movieDTOS);


//        List<Rating> ratingRdd = getRatingRDD(jsc);


    }

    private List<Rating> getRatingRDD(JavaSparkContext jsc) {
        return jsc.textFile(DatasetConstant.ratingsCvs)
                .map(RatingMapper::lineToRating)
                .collect();
    }

    private List<Movie> getLinksRDD(JavaSparkContext jsc) {
        return jsc.textFile(DatasetConstant.linksCvs)
                .map(MovieMapper::mergeLinks)
                .collect();
    }

    private List<Movie> getMovieRDD(JavaSparkContext jsc) {
        return jsc.textFile(DatasetConstant.moviesCvs)
                .map(MovieMapper::stringToMovie)
                .collect();
    }

    private void updateMovieFromApi(RestTemplate restTemplate, List<Movie> movies, List<MovieDTO> movieDTOS) {
        movies.parallelStream().filter(movie -> movie.getOverview()==null).forEach(
                movie -> {
                        ResponseEntity<MovieDTO> resultGoMonth = updateMovies(restTemplate, movies, movieDTOS,movie);
                        downloadMovieImage(movie, resultGoMonth);
                }
        );
    }

    private ResponseEntity<MovieDTO> updateMovies(RestTemplate restTemplate, List<Movie> movies, List<MovieDTO> movieDTOS,Movie movie) {

        String url = AppConstant.url+ movie.getTmdbId() + "?api_key="+ AppConstant.apiKey;
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        HttpEntity entity = new HttpEntity(httpHeaders);
        ResponseEntity<MovieDTO> resultGoMonth=null;
        try {
            resultGoMonth = Optional.of(restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    entity,
                    MovieDTO.class)).orElse(new ResponseEntity<>(HttpStatus.OK) );

            movieDTOS.add(resultGoMonth.getBody());
            movie.setUrl(resultGoMonth.getBody().getPoster_path());
            movie.setOverview(resultGoMonth.getBody().getOverview());
            movie.setReleaseDate(resultGoMonth.getBody().getRelease_date());
            movie.setHomepage(resultGoMonth.getBody().getHomepage());
            movie.setVideo(resultGoMonth.getBody().getVideo());

            movieRepository.save(movies);

            return resultGoMonth;
        }
        catch (final Exception e) {
            return new ResponseEntity<MovieDTO>(HttpStatus.NOT_FOUND);
        }

    }

    private void downloadMovieImage(Movie movie, ResponseEntity<MovieDTO> resultGoMonth) {
        String imageUrl = "https://image.tmdb.org/t/p/w1280" +movie.getUrl();
        if(resultGoMonth.getBody().getPoster_path() != null){
            String destinationFilePath = DatasetConstant.movieImageDestenation+resultGoMonth.getBody().getPoster_path().substring(1,resultGoMonth.getBody().getPoster_path().length()); // For windows something like c:\\path\to\file\test.jpg
            InputStream inputStream = null;
            try {
                inputStream = new URL(imageUrl).openStream();
                Files.copy(inputStream, Paths.get(destinationFilePath));
            } catch (IOException e) {
                throw new ImageNotReadException("input stream is not opened");
            }
            finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        throw new ImageNotReadException("input stream is not opened");
                    }
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
                    isActive=true;
                }
            }
            if (!isActive){
                movieRDD.get(i).setTmdbId("********");
                movieRDD.get(i).setImdbId("********");
                movieRDD.get(i).setUrl("********");
            }
            movieRepository.save(movieRDD.get(i));
        }
    }

}
