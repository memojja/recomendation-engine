import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import {CdkTableModule} from '@angular/cdk/table';
import {platformBrowserDynamic} from '@angular/platform-browser-dynamic';
import {BrowserAnimationsModule} from '@angular/platform-browser/animations';
import {HttpClient, HttpClientModule} from '@angular/common/http';
import {HttpModule} from "@angular/http";
import {NgbModule, NgbRatingConfig} from '@ng-bootstrap/ng-bootstrap';
import {FormsModule,ReactiveFormsModule} from '@angular/forms';
import {
  MatAutocompleteModule,
  MatButtonModule,
  MatButtonToggleModule,
  MatCardModule,
  MatCheckboxModule,
  MatChipsModule,
  MatDatepickerModule,
  MatDialogModule,
  MatDividerModule,
  MatExpansionModule,
  MatGridListModule,
  MatIconModule,
  MatInputModule,
  MatListModule,
  MatMenuModule,
  MatNativeDateModule,
  MatPaginatorModule,
  MatProgressBarModule,
  MatProgressSpinnerModule,
  MatRadioModule,
  MatRippleModule,
  MatSelectModule,
  MatSidenavModule,
  MatSliderModule,
  MatSlideToggleModule,
  MatSnackBarModule,
  MatSortModule,
  MatStepperModule,
  MatTableModule,
  MatTabsModule,
  MatToolbarModule,
  MatTooltipModule,
} from '@angular/material';
import { HTTP_INTERCEPTORS } from '@angular/common/http';

import {PizzaPartyComponent} from './components/movie-list/movie-list.component';
import { SearchbarComponent } from './components/searchbar/searchbar.component';
import { AppComponent } from './app.component';
import { MovieListComponent, DialogDataExampleDialog } from './components/movie-list/movie-list.component';
import { MovieDetailComponent } from './components/movie-detail/movie-detail.component';
import {MovieService} from './services/movie.service';
import { ProfileComponent } from './components/user/profile/profile.component';
import { LoginComponent } from './components/user/login/login.component';
import { RegisterComponent } from './components/user/register/register.component';
import {routing} from "./app.routing";

import {AccountService} from '../app/services/account.service';
import {UrlPermission} from '../app/urlPermission/url.permission';
import {AuthService} from '../app/services/auth.service';
import { MovieComponent } from './components/movie/movie.component';
import { AuthInterceptor } from '../app/interceptor/AuthInterceptor';


@NgModule({
  exports: [
    CdkTableModule,
    MatAutocompleteModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatCardModule,
    MatCheckboxModule,
    MatChipsModule,
    MatStepperModule,
    MatDatepickerModule,
    MatDialogModule,
    MatDividerModule,
    MatExpansionModule,
    MatGridListModule,
    MatIconModule,
    MatInputModule,
    MatListModule,
    MatMenuModule,
    MatNativeDateModule,
    MatPaginatorModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatRadioModule,
    MatRippleModule,
    MatSelectModule,
    MatSidenavModule,
    MatSliderModule,
    MatSlideToggleModule,
    MatSnackBarModule,
    MatSortModule,
    MatTableModule,
    MatTabsModule,
    MatToolbarModule,
    MatTooltipModule,
    
  ]
})
export class DemoMaterialModule {}


@NgModule({
  declarations: [
    AppComponent,
    MovieListComponent,
    MovieDetailComponent,
    ProfileComponent,
    PizzaPartyComponent,
    SearchbarComponent,
    DialogDataExampleDialog,
    LoginComponent, 
    RegisterComponent,
    MovieComponent
  ],
  imports: [
    BrowserModule, 
    FormsModule, 
    ReactiveFormsModule,
    HttpClientModule, 
    HttpModule,   
    DemoMaterialModule,
    NgbModule,
    BrowserAnimationsModule,
    routing
  ],
  
  entryComponents: [ 
    PizzaPartyComponent,
    SearchbarComponent,
    DialogDataExampleDialog
  
  ],
  providers: [MovieService, 
              HttpClient, 
              NgbRatingConfig,
              AuthService,
              AccountService,
              UrlPermission,
              { provide: HTTP_INTERCEPTORS, useClass: AuthInterceptor, multi: true }

            ],
  bootstrap: [AppComponent]
})
export class AppModule { }



platformBrowserDynamic().bootstrapModule(AppModule);
