import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';
import {Movie} from '../models/movie';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {tap} from 'rxjs/operators';
import {ApiUtil} from '../utils/api-util';
import {Rating} from '../models/rating';
import {User} from '../models/user';
import {AccountService} from './account.service';
import {Router} from '@angular/router';
import {PizzaPartyComponent} from '../components/movie-list/movie-list.component';
import {MatSnackBar} from '@angular/material';

@Injectable()
export class MovieService {
  apiUrls: ApiUtil = new ApiUtil;
  recomendationMovie: Movie[] = Array();
  selectedMovie : Movie[] = Array();
  recomendationList : Movie[] = Array();

  movie:Movie = new Movie;
  currentUser: User;
  currentMovie:Movie;
  constructor(private authService: AccountService, public router: Router,private http: HttpClient
    ,  public snackBar: MatSnackBar) {

    this.currentUser = authService.getCurrentUser();
  }


  openSnackBar() {
    this.snackBar.openFromComponent(PizzaPartyComponent, {
      duration: 2000,
    });
  }
  
  getMovies(): Observable<Movie[]> {
    return this.http.get<Movie[]>(this.apiUrls.getUrl(),
    // {
    //   headers: new HttpHeaders().set('Authorization', 'Basic ' +
    //   btoa(  this.currentUser.username+ ':' +  this.currentUser.password)),
    // }
  
  ).pipe(
      tap(movie => console.log('fetched ( getMovies )' + movie)),
    );
  }

  // getMoviesById(id: number): Observable<Movie> {
    getMoviesById(id: string): Observable<Movie> {
    return this.http.get<Movie>(this.apiUrls.getUrl() + '/' + id 
  
  ).pipe(
      tap(movie => console.log('fetched ( getMoviesById ) + ' + movie )),
    );
  }

  // this.http.get<Response>('https://api.handwriting.io/render/png?' + this.params,{
  //   headers: new HttpHeaders().set('7Q0ZBC889M7WRGRM','N6BQK4Z8PZ1HYYT4'),
  // }

  searchMovie(movieName: string): Observable<Movie[]> {
    console.log('loo')
    return this.http.get<Movie[]>(this.apiUrls.getUrl() + '/search?movieName=' + movieName
  //   ,{
  //   headers: this.getBasicAuthHeader(),
  // }
  ).pipe(
      tap(movie => console.log('fetched ( getMovies )' + movie)),
    );
  }

  recomendation(postMovieList: Rating[]) {
    console.log('recomendationss' + postMovieList);
     this.http.post('http://localhost:8081/recomendations', postMovieList)
      .subscribe(
        (states: Movie[]) => {
          states.forEach( state =>this.recomendationMovie.push(state) );
          // this.recomendationMovie = states;
            this.openSnackBar();
        }
      );
    return this.recomendationMovie;
  }

  getSelectedMovie(){
    return this.selectedMovie;
  }

  addSelectedMovie(movie:Movie){
    this.selectedMovie.push(movie);
  }


  getRecomendationList(){
    return this.recomendationMovie;
  }


  setCurrentMovie(movie:Movie){
    this.currentMovie=movie;
  }
  getCurrentMovie(){
    return this.currentMovie;
  }


}
