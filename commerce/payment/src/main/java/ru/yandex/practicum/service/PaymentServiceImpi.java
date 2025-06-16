package ru.yandex.practicum.service;

import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.exception.NotEnoughInformationException;
import ru.yandex.practicum.exception.ValidateException;
import ru.yandex.practicum.feign.client.OrderClient;
import ru.yandex.practicum.feign.client.ShoppingStoreClient;
import ru.yandex.practicum.mapper.PaymentMapper;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.PaymentRepository;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpi implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final PaymentMapper mapper;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    @Override
    public PaymentDto addPayment(OrderDto orderDto) {
        log.info("Запуск метода addPayment, на входе orderDto: {}", orderDto);
        validateOrder(orderDto);
        Payment payment = Payment.builder()
                .orderId(orderDto.getOrderId())
                .totalPayment(orderDto.getTotalPrice())
                .deliveryTotal(orderDto.getDeliveryPrice())
                .feeTotal(orderDto.getTotalPrice().multiply(BigDecimal.valueOf(0.1)))
                .paymentState(PaymentState.PENDING)
                .build();
        return mapper.paymentToPaymentDto(paymentRepository.save(payment));
    }

    @Override
    @Transactional
    public BigDecimal getTotalCost(OrderDto orderDto) {
        log.info("Запуск метода getTotalCost, на входе orderDto: {}", orderDto);
        if (orderDto.getDeliveryPrice() == null) {
            throw new ValidateException("В заказе недостаточно инофрмации для рассчёта");
        }
        return orderDto.getProductPrice()
                .add(orderDto.getProductPrice().multiply(BigDecimal.valueOf(0.1)))
                .add(orderDto.getDeliveryPrice());
    }

    @Override
    public void paymentSuccess(UUID orderId) {
        log.info("Запуск метода paymentSuccess, на входе orderDto: {}", orderId);
        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow(
                () -> new ValidateException("В заказе недостаточно инофрмации для рассчёта"));
        payment.setPaymentState(PaymentState.SUCCESS);
        orderClient.payment(payment.getOrderId());
    }

    @Override
    @Transactional(readOnly = true)
    public Double getPaymentProductCost(OrderDto orderDto) {
        log.info("Запуск метода getPaymentProductCost, на входе orderDto: {}", orderDto);
        double productCost = 0.0;
        Map<UUID, Long> products = orderDto.getProducts();
        if (products == null) {
            throw new ValidateException("В заказе недостаточно инофрмации для рассчёта");
        }
        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            ProductDto product = shoppingStoreClient.getProduct(entry.getKey());
            productCost += product.getPrice() * entry.getValue();
        }
        return productCost;
    }

    @Override
    public void paymentFailed(UUID orderId) {
        log.info("Запуск метода paymentFailed, на входе orderDto: {}", orderId);
        Payment payment = paymentRepository.findByOrderId(orderId).orElseThrow(
                () -> new ValidateException("В заказе недостаточно инофрмации для рассчёта"));
        payment.setPaymentState(PaymentState.FAILED);
        orderClient.paymentFailed(payment.getOrderId());
    }

    private void validateOrder(OrderDto orderDto) {
        if (orderDto.getDeliveryPrice() == null || orderDto.getProductPrice() == null || orderDto.getTotalPrice() == null) {
            throw new NotEnoughInformationException("В заказе недостаточно инофрмации для рассчёта");
        }
    }
}
