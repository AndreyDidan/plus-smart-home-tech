package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import jakarta.transaction.Transactional;
import ru.yandex.practicum.exception.*;
import ru.yandex.practicum.feign.client.WarehouseClient;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.mapper.CartMapper;
import ru.yandex.practicum.model.Cart;
import ru.yandex.practicum.repository.ShoppingCartRepository;


import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingCartServiceImpi implements ShoppingCartService {
    private final ShoppingCartRepository shoppingCartRepository;
    private final CartMapper mapper;
    private final WarehouseClient warehouseClient;

    @Override
    public ShoppingCartDto getShoppingCart(String username) {
        log.info("Запуск метода getShoppingCart, на входе username: {}", username);
        validUserName(username);
        return mapper.shoppingCartDtoToCart(shoppingCartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Корзины для пользователя " + username + " нет")));
    }

    @Transactional
    @Override
    public ShoppingCartDto addProduct(String username, Map<UUID, Long> cartProducts) {
        log.info("Запуск метода addProduct, на входе username :{}, cartProducts {}", username, cartProducts);
        validUserName(username);
        Cart shoppingCart = shoppingCartRepository.findByUsername(username)
                .orElse(shoppingCartRepository.save(Cart.builder()
                        .username(username)
                        .active(true)
                        .cartProducts(new HashMap<>())
                        .build()));
        if (shoppingCart.isActive()) {
            cartProducts.forEach((key, value) -> {
                shoppingCart.getCartProducts().put(key, value);
            });
            return mapper.shoppingCartDtoToCart(shoppingCartRepository.save(shoppingCart));
        } else {
            throw new ValidateException("Корзины для пользователя " + username + " нет");
        }
    }

    @Override
    public void deactivateCart(String username) {
        log.info("Запуск метода deactivateCart, на входе username: {}", username);
        validUserName(username);
        Cart cart = shoppingCartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Корзины для пользователя " + username + " нет"));
        cart.setActive(false);
        shoppingCartRepository.save(cart);
    }

    @Override
    public ShoppingCartDto removeProduct(String username, List<UUID> cartProducts) {
        log.info("Запуск метода removeProduct, на входе username:{}, cartProducts: {}", username, cartProducts);
        validUserName(username);
        Cart shoppingCart = shoppingCartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Корзины для пользователя " + username + " нет"));
        cartProducts.forEach(key -> {
            if (!shoppingCart.getCartProducts().containsKey(key)) {
                throw new ValidateException("Нет искомых товаров в корзине");
            }
        });

        cartProducts.forEach(uuid -> {
            shoppingCart.getCartProducts().remove(uuid);
        });

        return mapper.shoppingCartDtoToCart(shoppingCartRepository.save(shoppingCart));
    }

    @Transactional
    @Override
    public ShoppingCartDto changeQuantity(String username, ChangeProductQuantityRequest changeProductQuantityRequest) {
        log.info("Запуск метода changeQuantity, на входе username:{}, changeProductQuantityRequest: {}",
                username, changeProductQuantityRequest);
        validUserName(username);
        Cart cart = shoppingCartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Корзины для пользователя " + username + " нет"));
        cart.getCartProducts().put(changeProductQuantityRequest.getProductId(), changeProductQuantityRequest.getNewQuantity());
        Cart saveCart = shoppingCartRepository.save(cart);
        return mapper.shoppingCartDtoToCart(saveCart);

    }

    @Transactional
    @Override
    public BookedProductsDto bookShoppingCartInWarehouse(String username) {
        log.info("Запуск метода bookShoppingCartInWarehouse, на входе username: {}", username);
        validUserName(username);
        Cart cart = shoppingCartRepository.findByUsername(username)
                .orElseThrow(() -> new NotFoundException("Корзины для пользователя " + username + " нет"));

        log.info("получили корзину: {}", cart);
        return warehouseClient.checkProductQuantity(mapper.shoppingCartDtoToCart(cart));
    }

    private void validUserName(String username) {
        log.info("Проверяем валидность имени пользователя, на входе username: {}", username);
        if (username == null || username.isEmpty()) {
            throw new NotAuthorizedUserException("Имя пользователя не должно быть пустым.");
        }
    }
}