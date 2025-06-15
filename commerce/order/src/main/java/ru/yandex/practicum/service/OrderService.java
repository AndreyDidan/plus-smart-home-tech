package ru.yandex.practicum.service;

import ru.yandex.practicum.model.CreateNewOrderRequest;
import ru.yandex.practicum.model.OrderDto;
import ru.yandex.practicum.model.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

public interface OrderService {
    List<OrderDto> getOrderDto(String username);

    OrderDto addOrderDto(CreateNewOrderRequest createNewOrderRequest);

    OrderDto returnOrderDto(ProductReturnRequest productReturnRequest);

    OrderDto payment(UUID orderId);

    OrderDto paymentFailed(UUID orderId);

    OrderDto delivery(UUID orderId);

    OrderDto deliveryFailed(UUID orderId);

    OrderDto completed(UUID orderId);

    OrderDto calculateTotal(UUID orderId);

    OrderDto calculateDelivery(UUID orderId);

    OrderDto assembly(UUID orderId);

    OrderDto assemblyFailed(UUID orderId);
}
