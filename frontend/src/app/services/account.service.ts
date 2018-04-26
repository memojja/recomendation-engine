import { Injectable } from '@angular/core';
import {User} from "../models/user";
import {Http} from "@angular/http";
import {AppComponent} from "../app.component";
import { HttpClient } from '@angular/common/http';

@Injectable()
export class AccountService {

  constructor(private http:HttpClient) { }
  private currentUser:User;

  createAccount(user:User){
    this.currentUser=user;
    return this.http.post(AppComponent.API_URL+'/account/register',user);
      // .map( resp=> resp.json());
  }

  getCurrentUser(){
    return this.currentUser;
  }

}
