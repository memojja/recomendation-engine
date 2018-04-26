package com.example.movieservice.movieservice.registration.model;

import org.springframework.data.mongodb.core.mapping.Document;

import javax.persistence.*;
import java.util.UUID;

//@Entity
//@Table(name = "roles")
@Document(collection = "role")
public class Role {
    @Id
//    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Enumerated(EnumType.STRING)
//    @NaturalId
//    @Column(length = 60)
    private RoleName name;

    public Role() {
        id = UUID.randomUUID().toString();
    }

    public Role(RoleName name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public RoleName getName() {
        return name;
    }

    public void setName(RoleName name) {
        this.name = name;
    }
}