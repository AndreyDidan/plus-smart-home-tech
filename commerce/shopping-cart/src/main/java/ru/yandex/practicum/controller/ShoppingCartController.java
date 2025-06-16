package ru.yandex.practicum.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.feign.client.ShoppingCartClient;
import ru.yandex.practicum.model.BookedProductsDto;
import ru.yandex.practicum.model.ShoppingCartDto;
import ru.yandex.practicum.model.ChangeProductQuantityRequest;
import ru.yandex.practicum.service.ShoppingCartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-cart")
@AllArgsConstructor
public class ShoppingCartController implements ShoppingCartClient {
    private final ShoppingCartService shoppingCartService;

    @Override
    public ShoppingCartDto getShoppingCart(String username) {
        return shoppingCartService.getShoppingCart(username);
    }

    @Override
    public ShoppingCartDto addProductToCart(String username, Map<UUID, Long> products) {
        return shoppingCartService.addProduct(username, products);
    }

    @Override
    public void deactivateCart(String username) {
        shoppingCartService.deactivateCart(username);
    }

    @Override
    public ShoppingCartDto removeProductFromCart(String username, List<UUID> products) {
        return shoppingCartService.removeProduct(username, products);
    }

    @Override
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest changeProductQuantityRequest) {
        return shoppingCartService.changeQuantity(username, changeProductQuantityRequest);

    }

    @Override
    public BookedProductsDto bookProducts(String username) {
        return shoppingCartService.bookShoppingCartInWarehouse(username);
    }

    @Override
    public String getUserName(UUID cartId) {
        return shoppingCartService.getUserName(cartId);
    }
}
