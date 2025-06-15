package ru.yandex.practicum.service;

import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.model.PaymentDto;

import java.util.UUID;

public interface PaymentService {
    PaymentDto addPayment(OrderDto orderDto);

    Double getTotalCost(OrderDto orderDto);

    void paymentSuccess(UUID orderId);

    Double getPaymentProductCost(OrderDto orderDto);

    void paymentFailed(UUID orderId);
}
