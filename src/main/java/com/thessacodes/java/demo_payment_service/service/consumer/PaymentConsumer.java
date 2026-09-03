package com.thessacodes.java.demo_payment_service.service.consumer;

import com.thessacodes.java.demo_payment_service.model.PaymentCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {

    @KafkaListener(
            topics = "payment.created",
            groupId = "payment-service"
    )
    public void consume(PaymentCreatedEvent event) {
        //TODO Replace with Loggers
        System.out.println(
                "Processing payment: " + event.paymentId()
        );

        System.out.println(
                "Amount: " + event.amount() + " "
                        + event.currency()
        );

        //TODO Client Acknowledgement Mode

    }
}
