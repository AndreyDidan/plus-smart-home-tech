package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.Order;
import ru.yandex.practicum.model.OrderDto;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface OrderMapper {

    @Mapping(source = "state", target = "orderState")
    OrderDto orderToOrderDto(Order order);

    @Mapping(target = "username", ignore = true)
    @Mapping(source = "orderState", target = "state")
    Order orderDtoToOrder(OrderDto orderDto);
}
