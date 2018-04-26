package com.example.movieservice.movieservice.registration.model;

import com.example.movieservice.movieservice.registration.model.audit.DateAudit;
import org.hibernate.annotations.NaturalId;
import org.hibernate.validator.constraints.Email;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.*;

import javax.validation.constraints.Size;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

//@Entity
//@Table(name = "users", uniqueConstraints = {
//        @UniqueConstraint(columnNames = {
//                "username"
//        }),
//        @UniqueConstraint(columnNames = {
//                "email"
//        })
//})
@Document
public class User extends DateAudit {
    @Id
//    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

//    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer recomendation_id;

    @NotBlank
    @Size(max = 40)
    private String name;

    @NotBlank
    @Size(max = 15)
    private String username;

    @NaturalId
    @NotBlank
    @Size(max = 40)
    @Email
    private String email;

    @NotBlank
    @Size(max = 100)
    private String password;

    @ManyToMany(fetch = FetchType.LAZY)
//    @JoinTable(name = "user_roles",
//            joinColumns = @JoinColumn(name = "user_id"),
//            inverseJoinColumns = @JoinColumn(name = "role_id"))
    private Set<Role> roles = new HashSet<>();

    public User() {
        generateId();
    }

    private void generateId() {
        Random generator = new Random();
        recomendation_id = Math.abs(generator.nextInt()+7000);
        id =UUID.randomUUID().toString();
    }

    public User(String name, String username, String email, String password) {
        Random generator = new Random();
        this.name = name;
        this.username = username;
        this.email = email;
        this.password = password;
        generateId();

    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getRecomendation_id() {
        return recomendation_id;
    }

    public void setRecomendation_id(Integer recomendation_id) {
        this.recomendation_id = recomendation_id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Set<Role> getRoles() {
        return roles;
    }

    public void setRoles(Set<Role> roles) {
        this.roles = roles;
    }
}