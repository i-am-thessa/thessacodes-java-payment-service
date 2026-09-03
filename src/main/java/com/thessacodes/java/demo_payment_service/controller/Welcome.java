package com.thessacodes.java.demo_payment_service.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Welcome {

    @GetMapping("/")
    public String welcome() {
        return "Welcome to Demo Payment Service with Kafka Implementation!";
    }
}
