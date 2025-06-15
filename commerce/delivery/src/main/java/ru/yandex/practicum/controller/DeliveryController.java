package ru.yandex.practicum.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.feign.client.DeliveryClient;
import ru.yandex.practicum.model.DeliveryDto;
import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.service.DeliveryService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/delivery")
@AllArgsConstructor
public class DeliveryController implements DeliveryClient {

    private final DeliveryService deliveryService;

    @Override
    public DeliveryDto addDelivery(DeliveryDto deliveryDto) {
        return deliveryService.addDelivery(deliveryDto);
    }

    @Override
    public void successfulDelivery(UUID orderId) {
        deliveryService.successfulDelivery(orderId);
    }

    @Override
    public void pickedDelivery(UUID orderId) {
        deliveryService.pickedDelivery(orderId);
    }

    @Override
    public void failed(UUID orderId) {
        deliveryService.failed(orderId);
    }

    @Override
    public Double deliveryCost(OrderDto orderDto) {
        return deliveryService.deliveryCost(orderDto);
    }
}