import { Component, OnInit ,HostListener,Directive} from '@angular/core';
import {Movie} from '../../models/movie';
import {User} from '../../models/user';
import {Router} from '@angular/router';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-movie',
  templateUrl: './movie.component.html',
  styleUrls: ['./movie.component.css']
})
export class MovieComponent implements OnInit {
 
  title = 'Online Movie Recomendation Engine';
  static API_URL="http://localhost:8081";

  movie:Movie = new Movie;
  currentUser: User;
  constructor(private authService: AuthService, public router: Router) {
  }


  logout(){
    this.authService.logOut();
  }


  ngOnInit() {
  }

}

