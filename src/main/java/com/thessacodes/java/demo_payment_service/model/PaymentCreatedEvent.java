package com.thessacodes.java.demo_payment_service.model;

import java.math.BigDecimal;

public record PaymentCreatedEvent(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency
) {
}
