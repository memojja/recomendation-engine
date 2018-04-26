import { Injectable } from '@angular/core';
// import { Http, Headers, RequestOptions,Response} from '@angular/http';
import { Http, Headers, RequestOptions,Response} from '@angular/http';
import {BehaviorSubject,Observable} from "rxjs";

import 'rxjs/add/operator/map';

import {AppComponent} from "../app.component";
import {User} from "../models/user";
import { HttpClient } from '@angular/common/http';

@Injectable()
export class AuthService {

  isLoginPublisher = new BehaviorSubject<boolean>(this.getLocalData('token'))
  userPublisher = new BehaviorSubject<boolean>(this.getLocalData('user'))
  
    constructor(private http:Http) { }

  logIn(email, password): Observable<Object> {
    
    let loginUrl = `http://localhost:8081/api/auth/signin`

    return this.http.post(loginUrl, {
      "usernameOrEmail" : email,
      "password" : password
    } )
      .map(_res => _res.json())
      .map(_res => {
        let message = _res.message
        console.log(_res.accessToken);
        /** Eger bir token degeri donuyorsa!*/
        if (_res.accessToken) {
          this.saveToLocal('token', _res.accessToken)
      
          this.isLoginPublisher.next(true);
       
          return true
        }
        // response içinde bir token yok!
        return false
      })
  }

  logIn2(email,password) {

   return this.http.post('http://localhost:8081/api/auth/signin',{
      "usernameOrEmail" : email,
      "password" : password
    } );

  }

  
  register(user:User): Observable<boolean> {
    
    let loginUrl = `http://localhost:8081/api/auth/signup`

    return this.http.post(loginUrl, {
      "name": user.fullName,
      "username" : user.fullName,
      "email" : user.username,
      "password" :user.password
    })
      .map(_res => _res.json());
  }
  getToken(){
    return localStorage.getItem('token');
  }

  logOut(){
    localStorage.removeItem('token');
    
  }

  private getLocalData(name) : boolean {
    if(name =='token') return !!localStorage.getItem('token');

    let user = localStorage.getItem('user')
    if(user){
      return JSON.parse(user)
    }
    return null
  }

  private saveToLocal(name, data){
    localStorage.setItem(name,JSON.stringify(data))
  }



  // public logIn(user: User){

  //   let headers = new Headers();
  //   headers.append('Accept', 'application/json')
  //   // creating base64 encoded String from user name and password
  //   var base64Credential: string = btoa( user.username+ ':' + user.password);
  //   headers.append("Authorization", "Basic " + base64Credential);

  //   let options = new RequestOptions();
  //   options.headers=headers;

  //   return this.http.get(AppComponent.API_URL+"/account/login" ,   options)
  //     .map((response: Response) => {
  //     // login successful if there's a jwt token in the response
  //     let user = response.json().principal;// the returned user object is a principal object
  //     if (user) {
  //       // store user details  in local storage to keep user logged in between page refreshes
  //       localStorage.setItem('currentUser', JSON.stringify(user));
  //     }
  //   });
  // }
  // public getToken(): string {
  //   return localStorage.getItem('currentUser')
  // }

  // logOut() {
  //   // remove user from local storage to log user out
  //   return this.http.post(AppComponent.API_URL+"logout",{})
  //     .map((response: Response) => {
  //       localStorage.removeItem('currentUser');
  //     });

  // }

}
