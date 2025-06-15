package ru.yandex.practicum.feign.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.model.DeliveryDto;
import ru.yandex.practicum.model.OrderDto;

import java.util.UUID;

@FeignClient(name = "delivery", path = "/api/v1/delivery")
public interface DeliveryClient {
    @PutMapping
    DeliveryDto addDelivery(@RequestBody DeliveryDto deliveryDto);

    @PostMapping("/successful")
    void successfulDelivery(@RequestBody UUID orderId);

    @PostMapping("/picked")
    void pickedDelivery(@RequestBody UUID orderId);

    @PostMapping("/failed")
    void failed(@RequestBody UUID orderId);

    @PostMapping("/cost")
    Double deliveryCost(@RequestBody OrderDto orderDto);
}
