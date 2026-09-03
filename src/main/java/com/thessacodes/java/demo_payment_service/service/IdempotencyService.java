package com.thessacodes.java.demo_payment_service.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class IdempotencyService {

    //TODO this should be in the database or Redis
    private final Map<String, String> processedKeys =
            new ConcurrentHashMap<>();

    public boolean exists(String key) {
        return processedKeys.containsKey(key);
    }

    public void save(String key, String paymentId) {
        processedKeys.put(key, paymentId);
    }

    public String getPaymentId(String key) {
        return processedKeys.get(key);
    }
}
