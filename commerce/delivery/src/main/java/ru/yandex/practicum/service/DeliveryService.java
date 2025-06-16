package ru.yandex.practicum.service;

import ru.yandex.practicum.model.DeliveryDto;
import ru.yandex.practicum.model.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface DeliveryService {
    DeliveryDto addDelivery(DeliveryDto deliveryDto);

    void successfulDelivery(UUID orderId);

    void pickedDelivery(UUID orderId);

    void failed(UUID orderId);

    BigDecimal deliveryCost(OrderDto orderDto);
}
