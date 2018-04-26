import { Component, OnInit ,Input} from '@angular/core';
import { MovieService } from '../../services/movie.service';
import { Movie } from '../../models/movie';

@Component({
  selector: 'app-movie-detail',
  templateUrl: './movie-detail.component.html',
  styleUrls: ['./movie-detail.component.css']
})
export class MovieDetailComponent implements OnInit {


  // @Input() selectedMovie: Movie[];
  @Input('name') name: string;

  selectedMovie:Movie[] = Array();
  recomendations:Movie[] = Array();
  isRecomendation:Boolean = false;
  number:number = 1;
  constructor(private movieService:MovieService) { 
  }

  ngOnInit() {
    if(this.name =='selectedMovie'){
      this.number=this.number+1;
     this.isRecomendation=false;
      this.selectedMovie = this.movieService.getSelectedMovie();
      console.log(this.selectedMovie );

    }

    if(this.name =='recomendations'){
      this.isRecomendation= true;
      this.recomendations = this.movieService.getRecomendationList();
    }
    
  }
}
