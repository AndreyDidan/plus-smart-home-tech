package ru.yandex.practicum.service;

import ru.yandex.practicum.model.DeliveryDto;
import ru.yandex.practicum.model.OrderDto;

import java.util.UUID;

public interface DeliveryService {
    DeliveryDto addDelivery(DeliveryDto deliveryDto);

    void successfulDelivery(UUID orderId);

    void pickedDelivery(UUID orderId);

    void failed(UUID orderId);

    Double deliveryCost(OrderDto orderDto);
}
