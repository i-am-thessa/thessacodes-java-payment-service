package com.thessacodes.java.demo_payment_service.service;

import com.thessacodes.java.demo_payment_service.model.Payment;
import com.thessacodes.java.demo_payment_service.model.PaymentCreatedEvent;
import com.thessacodes.java.demo_payment_service.repository.PaymentRepository;
import com.thessacodes.java.demo_payment_service.service.producer.PaymentProducer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {
    private final PaymentProducer paymentProducer;
    private final IdempotencyService idempotencyService;
    private final PaymentRepository paymentRepository;

    //Use Constructor injection to support better mock testing
    public PaymentService(PaymentProducer paymentProducer,
                          IdempotencyService idempotencyService,
                          PaymentRepository paymentRepo) {
        this.paymentProducer = paymentProducer;
        this.idempotencyService = idempotencyService;
        this.paymentRepository = paymentRepo;
    }

    @Transactional
    public String createPayment(
            String idempotencyKey,
            Payment payment) {

        if (idempotencyService.exists(idempotencyKey)) {
            return "Payment already submitted: "
                    + idempotencyService.getPaymentId(idempotencyKey);
        }

        PaymentCreatedEvent event =
                new PaymentCreatedEvent(
                        payment.getPaymentId(),
                        payment.getCustomerId(),
                        payment.getAmount(),
                        payment.getCurrency()
                );

        // Save payment
        paymentRepository.save(payment);

        // Save idempotency record
        idempotencyService.save(
                idempotencyKey,
                payment.getPaymentId()
        );

        // Publish event
        paymentProducer.publish(event);

        return "Payment submitted: " + payment.getPaymentId();
    }
}
