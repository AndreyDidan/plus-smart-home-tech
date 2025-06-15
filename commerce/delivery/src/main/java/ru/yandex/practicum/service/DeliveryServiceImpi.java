package ru.yandex.practicum.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.exception.DeliveryAlreadyInDeliveryException;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.exception.ValidateException;
import ru.yandex.practicum.feign.client.OrderClient;
import ru.yandex.practicum.feign.client.WarehouseClient;
import ru.yandex.practicum.mapper.DeliveryMapper;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.DeliveryRepository;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpi implements DeliveryService {
    private static final Double BASE_PRICE = 5.0;
    private static final Double FRAGILE_PRICE = 0.2;
    private static final Double WEIGHT_PRICE = 0.3;
    private static final Double VOLUME_ACTION = 0.2;
    private static final Double IS_STREET_NOT_STREET_WAREHOUSE = 0.2;

    private final DeliveryRepository deliveryRepository;
    private final DeliveryMapper mapper;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;


    @Override
    public DeliveryDto addDelivery(DeliveryDto deliveryDto) {
        log.info("Запуск метода addDelivery, на входе deliveryDto: {}", deliveryDto);
        if (deliveryRepository.existsById(deliveryDto.getDeliveryId())) {
            throw new DeliveryAlreadyInDeliveryException("Доставка deliveryId:" +
                    deliveryDto.getDeliveryId() + " уже запланирована");
        }
        Delivery delivery = mapper.deliveryDtoToDelivery(deliveryDto);
        delivery.setDeliveryState(DeliveryState.CREATED);
        return mapper.deliveryToDeliveryDto(deliveryRepository.save(delivery));
    }

    @Override
    @Transactional
    public void successfulDelivery(UUID orderId) {
        log.info("Запуск метода successfulDelivery, на входе orderId: {}", orderId);
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.DELIVERED);
        orderClient.completed(delivery.getOrderId());
    }

    @Override
    @Transactional
    public void pickedDelivery(UUID orderId) {
        log.info("Запуск метода pickedDelivery, на входе orderId: {}", orderId);
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);
        ShippedToDeliveryRequest shippedToDeliveryRequest = new ShippedToDeliveryRequest(
                delivery.getOrderId(), delivery.getDeliveryId());
        warehouseClient.shippedDelivery(shippedToDeliveryRequest);
    }

    @Override
    @Transactional
    public void failed(UUID orderId) {
        log.info("Запуск метода failed, на входе orderId: {}", orderId);
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.FAILED);
        orderClient.deliveryFailed(orderId);
    }

    @Override
    @Transactional
    public Double deliveryCost(OrderDto orderDto) {
        log.info("Запуск метода deliveryCost, на входе orderDto: {}", orderDto);
        Delivery delivery = findByOrderId(orderDto.getOrderId());
        double priceDelivery;
        if (delivery.getFromAddress().getStreet().equals("ADDRESS_1")) {
            priceDelivery = BASE_PRICE * 1 + BASE_PRICE;
        } else if (delivery.getFromAddress().getStreet().equals("ADDRESS_2")) {
            priceDelivery = BASE_PRICE * 2 + BASE_PRICE;
        } else {
            throw new ValidateException("Работа с адресами кроме ADDRESS_1 и ADDRESS_2 не предусмотрена логикой программы");
        }
        if (orderDto.getFragile()) {
            priceDelivery = priceDelivery + priceDelivery * FRAGILE_PRICE;
        }
        priceDelivery = priceDelivery + orderDto.getDeliveryWeight() * WEIGHT_PRICE;
        priceDelivery = priceDelivery + orderDto.getDeliveryVolume() * VOLUME_ACTION;
        if (!delivery.getFromAddress().getStreet().equals(delivery.getToAddress().getStreet())) {
            priceDelivery = priceDelivery + priceDelivery * IS_STREET_NOT_STREET_WAREHOUSE;
        }
        return priceDelivery;
    }

    private Delivery findByOrderId(UUID orderId) {
        return deliveryRepository.findByOrderId(orderId).orElseThrow(
                () -> new NotFoundException("Доставка с заказом id = " + orderId + " не найдена"));
    }
}