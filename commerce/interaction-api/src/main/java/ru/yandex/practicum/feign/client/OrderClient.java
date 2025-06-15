package ru.yandex.practicum.feign.client;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.model.*;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order")
public interface OrderClient {
    @GetMapping
    List <OrderDto> getOrderDto(@RequestParam("username") String username);

    @PutMapping
    OrderDto addOrderDto(@RequestBody CreateNewOrderRequest createNewOrderRequest);

    @PostMapping("/return")
    OrderDto returnOrderDto(@RequestBody ProductReturnRequest productReturnRequest);

    @PostMapping("/payment")
    OrderDto payment(@RequestBody @NotNull UUID orderId);

    @PostMapping("/payment/failed")
    OrderDto paymentFailed(@RequestBody @NotNull UUID orderId);

    @PostMapping("/delivery")
    OrderDto delivery(@RequestBody @NotNull UUID orderId);

    @PostMapping("/delivery/failed")
    OrderDto deliveryFailed(@RequestBody @NotNull UUID orderId);

    @PostMapping("/completed")
    OrderDto completed(@RequestBody @NotNull UUID orderId);

    @PostMapping("/calculate/total")
    OrderDto calculateTotal(@RequestBody @NotNull UUID orderId);

    @PostMapping("/calculate/delivery")
    OrderDto calculateDelivery(@RequestBody @NotNull UUID orderId);

    @PostMapping("/assembly")
    OrderDto assembly(@RequestBody @NotNull UUID orderId);

    @PostMapping("/assembly/failed")
    OrderDto assemblyFailed(@RequestBody @NotNull UUID orderId);
}
