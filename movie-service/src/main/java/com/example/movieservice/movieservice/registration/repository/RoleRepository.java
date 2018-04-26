package com.example.movieservice.movieservice.registration.repository;



import com.example.movieservice.movieservice.registration.model.Role;
import com.example.movieservice.movieservice.registration.model.RoleName;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RoleRepository extends MongoRepository<Role, Long> {
    Optional<Role> findByName(RoleName roleName);
}