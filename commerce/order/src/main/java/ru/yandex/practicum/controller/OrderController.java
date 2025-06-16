package ru.yandex.practicum.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.feign.client.OrderClient;
import ru.yandex.practicum.model.CreateNewOrderRequest;
import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.model.ProductReturnRequest;
import ru.yandex.practicum.service.OrderService;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/order")
@AllArgsConstructor
public class OrderController implements OrderClient {

    private final OrderService orderService;

    @Override
    public List<OrderDto> getOrderDto(String username) {
        return orderService.getOrderDto(username);
    }

    @Override
    public OrderDto addOrderDto(CreateNewOrderRequest createNewOrderRequest) {
        return orderService.addOrderDto(createNewOrderRequest);
    }

    @Override
    public OrderDto returnOrderDto(ProductReturnRequest productReturnRequest) {
        return orderService.returnOrderDto(productReturnRequest);
    }

    @Override
    public OrderDto payment(UUID orderId) {
        return orderService.payment(orderId);
    }

    @Override
    public OrderDto paymentFailed(UUID orderId) {
        return orderService.paymentFailed(orderId);
    }

    @Override
    public OrderDto delivery(UUID orderId) {
        return orderService.delivery(orderId);
    }

    @Override
    public OrderDto deliveryFailed(UUID orderId) {
        return orderService.deliveryFailed(orderId);
    }

    @Override
    public OrderDto completed(UUID orderId) {
        return orderService.completed(orderId);
    }

    @Override
    public OrderDto calculateTotal(UUID orderId) {
        return orderService.calculateTotal(orderId);
    }

    @Override
    public OrderDto calculateDelivery(UUID orderId) {
        return orderService.calculateDelivery(orderId);
    }

    @Override
    public OrderDto assembly(UUID orderId) {
        return orderService.assembly(orderId);
    }

    @Override
    public OrderDto assemblyFailed(UUID orderId) {
        return orderService.assemblyFailed(orderId);
    }
}
