package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.Payment;
import ru.yandex.practicum.model.PaymentDto;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface PaymentMapper {
    PaymentDto paymentToPaymentDto(Payment payment);

    @Mapping(target = "orderId", ignore = true)
    @Mapping(target = "paymentState", ignore = true)
    Payment paymentDtoToPayment(PaymentDto paymentDto);
}
