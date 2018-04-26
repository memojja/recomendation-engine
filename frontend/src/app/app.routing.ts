import { Routes, RouterModule } from '@angular/router';
import {LoginComponent} from './components/user/login/login.component';
import {RegisterComponent} from './components/user/register/register.component'
import {ProfileComponent} from './components/user/profile/profile.component';
import {UrlPermission} from "./urlPermission/url.permission";
import { MovieComponent } from './components/movie/movie.component';


const appRoutes: Routes = [
  { path: 'profile', component: ProfileComponent ,canActivate: [UrlPermission] },
  { path: 'login', component: LoginComponent },
  { path: 'register', component: RegisterComponent },
  { path: 'movie',component:MovieComponent,canActivate: [UrlPermission] },
  // otherwise redirect to profile
  { path: '**', redirectTo: '/register' }
];

export const routing = RouterModule.forRoot(appRoutes);
