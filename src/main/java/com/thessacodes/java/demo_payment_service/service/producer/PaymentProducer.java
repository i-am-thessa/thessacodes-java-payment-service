package com.thessacodes.java.demo_payment_service.service.producer;

import com.thessacodes.java.demo_payment_service.model.PaymentCreatedEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class PaymentProducer {
    private final KafkaTemplate<String, PaymentCreatedEvent> kafkaTemplate;

    public PaymentProducer(
            KafkaTemplate<String, PaymentCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(PaymentCreatedEvent event) {

        kafkaTemplate.send(
                "payment.created",
                event.paymentId(),
                event
        );
    }
}
