package ru.yandex.practicum.service;

import ru.yandex.practicum.model.BookedProductsDto;
import ru.yandex.practicum.model.ChangeProductQuantityRequest;
import ru.yandex.practicum.model.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ShoppingCartService {
    ShoppingCartDto getShoppingCart(String username);

    ShoppingCartDto addProduct(String username, Map<UUID, Long> products);

    void deactivateCart(String username);

    ShoppingCartDto removeProduct(String username, List<UUID> cartProducts);

    ShoppingCartDto changeQuantity(String username, ChangeProductQuantityRequest changeProductQuantityRequest);

    BookedProductsDto bookShoppingCartInWarehouse(String username);

    String getUserName(UUID cartId);
}