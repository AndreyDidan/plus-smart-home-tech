package ru.yandex.practicum.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.feign.client.PaymentClient;
import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.model.PaymentDto;
import ru.yandex.practicum.service.PaymentService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payment")
@AllArgsConstructor
public class PaymentController implements PaymentClient {

    private final PaymentService paymentService;

    @Override
    public PaymentDto addPayment(OrderDto orderDto) {
        return paymentService.addPayment(orderDto);
    }

    @Override
    public Double getTotalCost(OrderDto orderDto) {
        return paymentService.getTotalCost(orderDto);
    }

    @Override
    public void paymentSuccess(UUID orderId) {
        paymentService.paymentSuccess(orderId);
    }

    @Override
    public Double getPaymentProductCost(OrderDto orderDto) {
        return paymentService.getPaymentProductCost(orderDto);
    }

    @Override
    public void paymentFailed(UUID orderId) {
        paymentService.paymentFailed(orderId);
    }
}
