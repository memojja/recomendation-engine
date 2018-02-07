import { Injectable } from '@angular/core';
import {Observable} from "rxjs/Observable";
import {Movie} from "../models/movie";
import {HttpClient} from "@angular/common/http";
import {tap} from "rxjs/operators";
import {ApiUtil} from "../utils/api-util";

@Injectable()
export class MovieService {

  apiUrls:ApiUtil = new ApiUtil;

  constructor(private http:HttpClient) { }


  getMovies():Observable<Movie[]>{
    return this.http.get<Movie[]>(this.apiUrls.getUrl()).pipe(
      tap(movie => console.log('fetched ' + movie)),
    );
  }

  getMoviesById(id:number):Observable<Movie>{
    return this.http.get<Movie>(this.apiUrls.getUrl() + '/'+id).pipe(
      tap(movie => console.log('fetched '+movie)),
    );
  }



}
