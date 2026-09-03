package com.thessacodes.java.demo_payment_service.repository;

import com.thessacodes.java.demo_payment_service.model.Payment;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {
}
