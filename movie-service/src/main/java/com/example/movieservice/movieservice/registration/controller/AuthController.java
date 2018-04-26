package com.example.movieservice.movieservice.registration.controller;



import com.example.movieservice.movieservice.registration.exception.AppException;
import com.example.movieservice.movieservice.registration.model.Role;
import com.example.movieservice.movieservice.registration.model.RoleName;
import com.example.movieservice.movieservice.registration.model.User;
import com.example.movieservice.movieservice.registration.payload.ApiResponse;
import com.example.movieservice.movieservice.registration.payload.JwtAuthenticationResponse;
import com.example.movieservice.movieservice.registration.payload.LoginRequest;
import com.example.movieservice.movieservice.registration.payload.SignUpRequest;
import com.example.movieservice.movieservice.registration.repository.RoleRepository;
import com.example.movieservice.movieservice.registration.repository.UserRepository;
import com.example.movieservice.movieservice.registration.security.JwtTokenProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.validation.Valid;
import java.net.URI;
import java.util.Collections;

@RestController
@RequestMapping("/api/auth")
@CrossOrigin("*")
public class AuthController {

    private final AuthenticationManager authenticationManager;
    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider tokenProvider;

    @Autowired
    public AuthController(AuthenticationManager authenticationManager, UserRepository userRepository, RoleRepository roleRepository, PasswordEncoder passwordEncoder, JwtTokenProvider tokenProvider) {
        this.authenticationManager = authenticationManager;
        this.userRepository = userRepository;
        this.roleRepository = roleRepository;
        this.passwordEncoder = passwordEncoder;
        this.tokenProvider = tokenProvider;
    }

    @PostMapping("/signin")
    public ResponseEntity<?> authenticateUser(@Valid @RequestBody LoginRequest loginRequest) {

        Authentication authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(
                        loginRequest.getUsernameOrEmail(),
                        loginRequest.getPassword()
                )
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);
        String jwt = tokenProvider.generateToken(authentication);
        return ResponseEntity.ok(new JwtAuthenticationResponse(jwt));
    }

    @PostMapping("/signup")
    public ResponseEntity<?> registerUser(@Valid @RequestBody SignUpRequest signUpRequest) {
        if(userRepository.existsByUsername(signUpRequest.getUsername()))
            return getResponse(new ApiResponse(false, "Username is already taken!"));

        if(userRepository.existsByEmail(signUpRequest.getEmail()))
            return getResponse(new ApiResponse(false, "Email Address already in use!"));

        User result = createUserAndSave(signUpRequest);

        URI location = ServletUriComponentsBuilder
                .fromCurrentContextPath().path("/users/{username}")
                .buildAndExpand(result.getUsername()).toUri();

        return ResponseEntity.created(location).body(new ApiResponse(true, "User registered successfully"));
    }

    private User createUserAndSave(@Valid @RequestBody SignUpRequest signUpRequest) {
        User user = new User(signUpRequest.getName(), signUpRequest.getUsername(),
                signUpRequest.getEmail(), signUpRequest.getPassword());

        user.setPassword(passwordEncoder.encode(user.getPassword()));

        Role userRole = roleRepository.findByName(RoleName.ROLE_USER)
                .orElseThrow(() -> new AppException("User Role not set."));

        user.setRoles(Collections.singleton(userRole));

        return userRepository.save(user);
    }

    private ResponseEntity getResponse(ApiResponse apiResponse) {
        return new ResponseEntity(apiResponse,
                HttpStatus.BAD_REQUEST);
    }
}