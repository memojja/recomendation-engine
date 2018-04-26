import { Component, OnInit } from '@angular/core';
import {User} from '../../../models/user';
import {AuthService} from '../../../services/auth.service';
import {Router} from "@angular/router";

@Component({
  selector: 'app-profile',
  templateUrl: './profile.component.html',
  styleUrls: ['./profile.component.css']
})
export class ProfileComponent implements OnInit {

  currentUser: User;
  constructor(public authService: AuthService, public router: Router) {
    this.currentUser = JSON.parse(localStorage.getItem('currentUser'));
  }

  ngOnInit() {
  }

// login out from the app
  logOut() {
    this.authService.logOut();
      // .subscribe(
      //   data => {
      //     this.router.navigate(['/login']);
      //   },
      //   error => {

      //   });
  }

}
