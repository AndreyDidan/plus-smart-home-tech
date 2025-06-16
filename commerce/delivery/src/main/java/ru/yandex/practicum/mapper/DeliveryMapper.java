package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.Delivery;
import ru.yandex.practicum.model.DeliveryDto;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface DeliveryMapper {
    DeliveryDto deliveryToDeliveryDto(Delivery delivery);

    @Mapping(target = "fromAddress.addressId", ignore = true)
    @Mapping(target = "toAddress.addressId", ignore = true)
    Delivery deliveryDtoToDelivery(DeliveryDto deliveryDto);
}
