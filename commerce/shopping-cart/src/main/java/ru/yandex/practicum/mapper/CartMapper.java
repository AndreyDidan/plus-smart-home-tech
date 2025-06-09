package ru.yandex.practicum.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import ru.yandex.practicum.model.ShoppingCartDto;
import ru.yandex.practicum.model.Cart;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface CartMapper {

    @Mapping(source = "cartId", target = "shoppingCartId")
    @Mapping(source = "cartProducts", target = "products")
    ShoppingCartDto shoppingCartDtoToCart (Cart cart);
}