import { Component, OnInit } from '@angular/core';
import {User} from '../../../models/user';
import {AuthService} from '../../../services/auth.service';
import {Router} from "@angular/router";
import {AccountService} from '../../../services/account.service';

@Component({
  selector: 'app-register',
  templateUrl: './register.component.html',
  styleUrls: ['./register.component.css']
})
export class RegisterComponent implements OnInit {

  user: User = new User();
  errorMessage: string;

  constructor(public accountService: AccountService, public router: Router,public service:AuthService) {
  }

  ngOnInit() {
  }

  register() {

    this.service.register(this.user)
      .subscribe(data=>{
        this.router.navigate(['/movie']);
        },err=>{
        this.errorMessage="error :  Username or password is incorrect";
        }
      )

    // this.accountService.createAccount(this.user).subscribe(data => {
    //     this.router.navigate(['/login']);
    //   }, err => {
    //     console.log(err);
    //     this.errorMessage = "username already exist";
    //   }
    // )
  }

  

}
