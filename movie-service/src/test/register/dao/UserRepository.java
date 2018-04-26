package com.example.movieservice.movieservice.register.dao;



import com.example.movieservice.movieservice.register.entities.User;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

/**
 * @author kamal berriga
 *
 */
/* this the user  Repository interface  */ 
@Repository
public interface UserRepository extends MongoRepository<User, Long> {

public User findOneByUsername(String username);
}
