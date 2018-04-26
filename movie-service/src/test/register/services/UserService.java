package com.example.movieservice.movieservice.register.services;


import com.example.movieservice.movieservice.register.dao.UserRepository;
import com.example.movieservice.movieservice.register.entities.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Random;

/**
 * @author kamal berriga
 *
 */
@Service
public class UserService {

	@Autowired
	private UserRepository userRepository;

	private Random randomGenerator = new Random();

	public User save(User newUser) {
		newUser.setId(randomGenerator.nextInt()+7500);
		newUser.setRole("USER");
		return userRepository.save(newUser);
	}

	public User update(User user) {
		return userRepository.save(user);
	}

	public User find(String userName) {
		return userRepository.findOneByUsername(userName);
	}

	public User find(Long id) {
		return userRepository.findOne(id);
	}
}
