import { Component, OnInit } from '@angular/core';
import {MovieService} from "../../services/movie.service";
import {Movie} from "../../models/movie";

@Component({
  selector: 'app-movie-list',
  templateUrl: './movie-list.component.html',
  styleUrls: ['./movie-list.component.css']
})
export class MovieListComponent implements OnInit {

  movieList:Movie[];
  selectedMovie:Movie;

  constructor(private service:MovieService) { }


  ngOnInit() {
    this.getMovies();
  }

  getMovies(){
    this.service.getMovies().subscribe(movies => this.movieList = movies);
  }

  getMovieById(id:number){
    console.log('ddddddddddd'+id);
    this.service.getMoviesById(id).subscribe(movie => this.selectedMovie = movie);
  }

}
