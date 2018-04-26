import { Injectable,Inject } from '@angular/core';
import { HttpInterceptor, HttpHandler, HttpRequest, HttpEvent, HttpResponse ,HttpHeaders}
  from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/do';
import {User} from '../models/user';
import {AuthService} from '../services/auth.service';
import {AccountService} from '../services/account.service';

@Injectable()
export class AuthInterceptor implements HttpInterceptor {
    currentUser: User;
    constructor (private auth:AuthService,private account:AccountService) {}

  intercept(
    req: HttpRequest<any>,
    next: HttpHandler
  ): Observable<HttpEvent<any>> {

  
       let duplicate;
        var earer = 'Bearer ';
        let local = localStorage.getItem('token').substr(1,localStorage.getItem('token').length-2);
        
      if ( localStorage.getItem('token')) {
          let auth = 'Bearer ' + local;
         duplicate = req.clone({ 
            setHeaders: {
                Authorization: auth
              }
          
         });
    }


    console.log(duplicate);


         return next.handle(duplicate);
      }
}

// }