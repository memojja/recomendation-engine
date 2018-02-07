import { Component } from '@angular/core';
import {Movie} from "./models/movie";

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'MovieList';

  movie:Movie = new Movie;


  constructor(){
  }

  handler(movie:Movie){
    console.log(movie.name);
    console.log('sadsad');
  }

}
