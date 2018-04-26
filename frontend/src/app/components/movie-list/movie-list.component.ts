import { Component, OnInit,Inject,ViewChild } from '@angular/core';
import {MovieService} from '../../services/movie.service';
import {Movie} from '../../models/movie';
import { DomSanitizer } from '@angular/platform-browser';
import {Rating} from '../../models/rating';
import {MatDialog, MAT_DIALOG_DATA,MatDialogConfig} from '@angular/material';
import {MatSnackBar} from '@angular/material';
import {User} from '../../models/user';
import {AuthService} from '../../services/auth.service';
import {Router} from '@angular/router';
import {MatPaginator, MatSort, MatTableDataSource} from '@angular/material';

@Component({
  selector: 'app-movie-list',
  templateUrl: './movie-list.component.html',
  styleUrls: ['./movie-list.component.css']
})
export class MovieListComponent implements OnInit {

  isDisabled:Boolean = true;
  movieList: Movie[];
  recomendationMovie: Movie[] = new Array();
  postMovieList: Array<Rating> = new Array();
  selectedMovieList:Array<Movie> = new Array();

  searchedMovie: Movie[];
  selectedMovie: Movie;
  movieName: string;
  modal: string;
  sanitizedUrl;
  currentRate = 10;
  isSelectMovie: boolean;
  movie:Movie = new Movie;
  currentUser: User;
  isRecomendationProcessing:Boolean = false;

  dataSource : MatTableDataSource<Movie>;
  displayedColumns = ['id', 'name', 'progress', 'color'];
  changedMovie:Movie;


  @ViewChild(MatPaginator) paginator: MatPaginator;
  @ViewChild(MatSort) sort: MatSort;

  constructor(
    private movieService :MovieService,
    public snackBar: MatSnackBar,
    private service: MovieService, 
    private sanitizer: DomSanitizer,
    public dialog: MatDialog,
    private authService: AuthService, 
    public router: Router) { 
      // this.currentUser = JSON.parse(localStorage.getItem('currentUser'));  
  }
  
  ngOnInit() {
    this.isSelectMovie = true;
  }

  applyFilter(filterValue: string) {
    this.searchMovie(filterValue);
  }
  async  changeSelect(movie:Movie){

    console.log('workinggg');
    console.log(movie);
    
    this.changedMovie=movie;
    await this.getMovieById(movie.movieId,movie.url);
    
  }

  openDialog(movie:Movie) {
    this.movieService.setCurrentMovie(this.changedMovie);


    let config: MatDialogConfig = {
      disableClose: false,
      width: '600px',
      height: '600px',
      // position: {
      //   top: '',
      //   bottom: '',
      //   left: '',
      //   right: ''
      // },
      data:  movie
    };
    

    let dialog  = this.dialog.open(DialogDataExampleDialog, 
      config
      // { data:movie}
  );

    dialog.afterClosed()
      .subscribe(selection => {
        if (selection) {
          this.postRating();
        } else {
          // User clicked 'Cancel' or clicked outside the dialog
        }
      });
  }
  
  
  openSnackBar() {
    this.snackBar.openFromComponent(PizzaPartyComponent, {
      duration: 500,
    });
  }
  


  getMovies() {
    this.service.getMovies()
                  .subscribe(movies => this.movieList = movies);
  }

  // getMovieById(id: number, url: string) {
   getMovieById(id: string, url: string) {
    this.service.getMoviesById(id).subscribe(movie => {
      this.selectedMovie = movie;
      const url3: string = './assets/recomendation-images' + url;
      this.sanitizedUrl = url3;
      // this.sanitizedUrl = this.sanitizer.bypassSecurityTrustUrl(url3);
    });
  }

  searchMovie(movieName: string) {

    console.log('searchMovie is entered by ' + movieName);
    if (movieName.length > 2) {
        this.service.searchMovie(movieName).subscribe(movies => {
          this.searchedMovie = movies;
          this.dataSource = new MatTableDataSource(movies);
          this.dataSource.filter = movieName.trim().toLowerCase();
          this.dataSource.paginator = this.paginator;
          this.dataSource.sort = this.sort;
      });
    }
    this.modal = 'modal on';

  }


  postRating() {
    let ratingg = new Rating();
    let movie = new Movie();
    ratingg.movieId = this.selectedMovie.movieId;
    ratingg.rating = (this.currentRate/2).toString();
     this.postMovieList.push(ratingg);
     movie = this.selectedMovie;
     console.log('selected movie : ');
     console.log(movie);
    // movie.name = this.selectedMovie.name;
    movie.url = './assets/recomendation-images' + this.selectedMovie.url;
    this.selectedMovieList.push(movie);
    this.service.addSelectedMovie(movie);
    if(this.selectedMovieList.length == 5) this.recomendation();
    if(this.selectedMovieList.length > 6) this.isDisabled= false;
  }

  recomendation() {
    this.isRecomendationProcessing=true;
    this.recomendationMovie =  this.service.recomendation(this.postMovieList);

  }

  updateSelectedMovie(movie: Movie){
    this.selectedMovie.movieId = movie.movieId;
  }

}


@Component({
  selector: 'snack-bar-component-example-snack',
  templateUrl: 'snack-bar-component-example-snack.html',
  styles: [`.example-pizza-party { color: hotpink; }`],
})
export class PizzaPartyComponent {}



import { MatDialogRef } from '@angular/material';

@Component({
  selector: 'dialog-data-example-dialog',
  templateUrl: 'dialog-data-example-dialog.html',
})
export class DialogDataExampleDialog  implements OnInit{
  // constructor(@Inject(MAT_DIALOG_DATA) public data: any) {}
  constructor(public dialogRef: MatDialogRef<DialogDataExampleDialog>,
              private movieService:MovieService,
              @Inject(MAT_DIALOG_DATA) public data: any) { }

  currentRate:number;
  selectedMovie:Movie;
  
  
  ngOnInit(){
    console.log('asas');
    console.log(this.data);
      // this.selectedMovie = this.movieService.getCurrentMovie();

  }

  confirmSelection() {
    this.dialogRef.close(this.currentRate);

  }

}

