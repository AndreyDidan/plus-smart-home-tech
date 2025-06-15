package ru.yandex.practicum.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.weaver.ast.Or;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.exception.NotAuthorizedUserException;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.feign.client.DeliveryClient;
import ru.yandex.practicum.feign.client.PaymentClient;
import ru.yandex.practicum.feign.client.WarehouseClient;
import ru.yandex.practicum.mapper.OrderMapper;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.OrderRepository;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpi implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderMapper mapper;
    private final DeliveryClient deliveryClient;
    private final PaymentClient paymentClient;
    private final WarehouseClient warehouseClient;

    @Override
    public List<OrderDto> getOrderDto(String username) {
        log.info("Запуск метода getOrderDto, на входе username: {}", username);
        if (username == null || username.isEmpty()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым.");
        }
        return orderRepository.findByUsername(username).stream()
                .map(mapper::orderToOrderDto).toList();
    }

    @Override
    @Transactional
    public OrderDto addOrderDto(CreateNewOrderRequest createNewOrderRequest) {
        log.info("Запуск метода addOrderDto, на входе username: {}", createNewOrderRequest);
        BookedProductsDto bookedProducts = warehouseClient.checkProductQuantity(createNewOrderRequest.getShoppingCart());
        Order order = Order.builder()
                .shoppingCartId(createNewOrderRequest.getShoppingCart().getShoppingCartId())
                .products(createNewOrderRequest.getShoppingCart().getProducts())
                .state(OrderState.NEW)
                .build();
        Order newOrder = orderRepository.save(order);
        order = orderRepository.save(order);

        AddressDto warehouseAddress = warehouseClient.getAddress();
        DeliveryDto newDelivery = DeliveryDto.builder()
                .fromAddress(warehouseAddress)
                .toAddress(createNewOrderRequest.getDeliveryAddress())
                .orderId(order.getOrderId())
                .deliveryState(DeliveryState.CREATED)
                .build();
        newDelivery = deliveryClient.addDelivery(newDelivery);
        order.setDeliveryId(newDelivery.getDeliveryId());

        order = orderRepository.save(order);
        log.info("New order is saved: {}", order);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto returnOrderDto(ProductReturnRequest productReturnRequest) {
        log.info("Запуск метода returnOrderDto, на входе username: {}", productReturnRequest);
        Order order = orderRepository.findById(productReturnRequest.getOrderId())
                .orElseThrow(() -> new NotFoundException("Заказ не найден"));
        warehouseClient.productToWarehouse(productReturnRequest.getProducts());
        order.setState(OrderState.PRODUCT_RETURNED);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto payment(UUID orderId) {
        log.info("Запуск метода payment, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.PAID);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto paymentFailed(UUID orderId) {
        log.info("Запуск метода paymentFailed, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto delivery(UUID orderId) {
        log.info("Запуск метода delivery, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.DELIVERED);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto deliveryFailed(UUID orderId) {
        log.info("Запуск метода deliveryFailed, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto completed(UUID orderId) {
        log.info("Запуск метода completed, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.COMPLETED);
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto calculateTotal(UUID orderId) {
        log.info("Запуск метода calculateTotal, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setTotalPrice(paymentClient.getTotalCost(mapper.orderToOrderDto(order)));
        return mapper.orderToOrderDto(order);
    }

    @Override
    public OrderDto calculateDelivery(UUID orderId) {
        log.info("Запуск метода calculateDelivery, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        Double totalPrice = deliveryClient.deliveryCost(mapper.orderToOrderDto(order));
        order.setTotalPrice(totalPrice);
        return mapper.orderToOrderDto(orderRepository.save(order));
    }

    @Override
    public OrderDto assembly(UUID orderId) {
        log.info("Запуск метода assembly, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        AssemblyProductsForOrderRequest assemblyRequest = AssemblyProductsForOrderRequest.builder()
                .orderId(orderId)
                .products(order.getProducts())
                .build();
        warehouseClient.orderAssembly(assemblyRequest);
        order.setState(OrderState.ASSEMBLED);
        return mapper.orderToOrderDto(orderRepository.save(order));
    }

    @Override
    public OrderDto assemblyFailed(UUID orderId) {
        log.info("Запуск метода assemblyFailed, на входе username: {}", orderId);
        Order order = getOrder(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        return mapper.orderToOrderDto(orderRepository.save(order));
    }

    private Order getOrder(UUID orderId) {
        return orderRepository.findById(orderId).orElseThrow(() ->
                new NotFoundException("Заказ не найден"));
    }
}
