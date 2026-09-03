package com.thessacodes.java.demo_payment_service.controller;

import com.thessacodes.java.demo_payment_service.model.Payment;
import com.thessacodes.java.demo_payment_service.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<String> createPayment(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody Payment payment) {

        String result = paymentService.createPayment(
                idempotencyKey,
                payment
        );

        return ResponseEntity.ok(result);
    }
}