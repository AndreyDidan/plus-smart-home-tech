package ru.yandex.practicum.feign.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.model.PaymentDto;

import java.util.UUID;

@FeignClient(name = "payment", path = "/api/v1/payment")
public interface PaymentClient {

    @PostMapping
    PaymentDto addPayment(@RequestBody OrderDto orderDto);

    @PostMapping("/totalCost")
    Double getTotalCost(@RequestBody OrderDto orderDto);

    @PostMapping("/refund")
    void paymentSuccess(@RequestBody UUID orderId);

    @PostMapping("/productCost")
    Double getPaymentProductCost(@RequestBody OrderDto orderDto);

    @PostMapping("/failed")
    void paymentFailed(@RequestBody UUID orderId);
}
