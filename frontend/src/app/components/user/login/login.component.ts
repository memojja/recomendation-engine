import { Component, OnInit } from '@angular/core';
import {User} from '../../../models/user';
import {AuthService} from '../../../services/auth.service';
import {Router} from "@angular/router";

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit {

  user: User=new User();
  errorMessage:string;
  constructor(private authService :AuthService, private router: Router) { }



  ngOnInit() {
  }

  login(){
    // console.log(this.user);

    this.authService.logIn(this.user.username,this.user.password)
      .subscribe(data=>{
        this.router.navigate(['/movie']);
        },err=>{
        this.errorMessage="error :  Username or password is incorrect";
        }
      );



      // this.authService.logIn(this.user.username,this.user.password).subscribe(
      //   data=>{
      //     //     localStorage.setItem(name,JSON.stringify(data))

      //     // data.accessToken

      //   this.router.navigate(['/movie']);
      //   },err=>{
      //   this.errorMessage="error :  Username or password is incorrect";
      //   }
      // );


  

  }


}
