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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpi implements DeliveryService {
    private static final BigDecimal BASE_PRICE = BigDecimal.valueOf(5.0);
    private static final BigDecimal FRAGILE_PRICE = BigDecimal.valueOf(0.2);
    private static final BigDecimal WEIGHT_PRICE = BigDecimal.valueOf(0.3);
    private static final BigDecimal VOLUME_ACTION = BigDecimal.valueOf(0.2);
    private static final BigDecimal IS_STREET_NOT_STREET_WAREHOUSE = BigDecimal.valueOf(0.2);

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
        orderClient.assembly(orderId);
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
    public BigDecimal deliveryCost(OrderDto orderDto) {
        log.info("Запуск метода deliveryCost, на входе orderDto: {}", orderDto);
        Delivery delivery = findByOrderId(orderDto.getOrderId());
        BigDecimal priceDelivery;

        if (delivery.getFromAddress().getStreet().equals("ADDRESS_1")) {
            priceDelivery = BASE_PRICE.multiply(BigDecimal.valueOf(1)).add(BASE_PRICE);
        } else if (delivery.getFromAddress().getStreet().equals("ADDRESS_2")) {
            priceDelivery = BASE_PRICE.multiply(BigDecimal.valueOf(2)).add(BASE_PRICE);
        } else {
            throw new ValidateException("Работа с адресами кроме ADDRESS_1 и ADDRESS_2 не предусмотрена логикой программы");
        }

        if (orderDto.getFragile()) {
            priceDelivery = priceDelivery.add(priceDelivery.multiply(FRAGILE_PRICE));
        }
        priceDelivery = priceDelivery.add(BigDecimal.valueOf(orderDto.getDeliveryWeight()).multiply(WEIGHT_PRICE));
        priceDelivery = priceDelivery.add(BigDecimal.valueOf(orderDto.getDeliveryVolume()).multiply(VOLUME_ACTION));

        if (!delivery.getFromAddress().getStreet().equals(delivery.getToAddress().getStreet())) {
            priceDelivery = priceDelivery.add(priceDelivery.multiply(IS_STREET_NOT_STREET_WAREHOUSE));
        }

        return priceDelivery.setScale(2, RoundingMode.HALF_UP);  // Установите нужный scale для округления
    }

    private Delivery findByOrderId(UUID orderId) {
        return deliveryRepository.findByOrderId(orderId).orElseThrow(
                () -> new NotFoundException("Доставка с заказом id = " + orderId + " не найдена"));
    }
}