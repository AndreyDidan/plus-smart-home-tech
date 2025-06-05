package ru.yandex.practicum.controller;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.feign.client.ShoppingStoreClient;
import ru.yandex.practicum.model.Pageable;
import ru.yandex.practicum.model.ProductCategory;
import ru.yandex.practicum.model.ProductDto;
import ru.yandex.practicum.model.SetProductQuantityStateRequest;
import ru.yandex.practicum.service.ShoppingStoreService;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/shopping-store")
@AllArgsConstructor
public class ShoppingProductController implements ShoppingStoreClient {
    private final ShoppingStoreService shoppingStoreService;

    @Override
    public Page<ProductDto> getProducts(ProductCategory category, @Valid Pageable pageable) {
        return shoppingStoreService.getProductPage(category, pageable);
    }

    @Override
    public ProductDto addProduct(ProductDto productDto) {
        return shoppingStoreService.addProduct(productDto);
    }

    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        return shoppingStoreService.updateProduct(productDto);
    }

    @Override
    public boolean removeProduct(UUID productId) {
        return shoppingStoreService.deleteProduct(productId);
    }

    @Override
    public boolean setProductQuantityState(SetProductQuantityStateRequest setProductQuantityStateRequest) {
        return shoppingStoreService.setProductQuantityState(setProductQuantityStateRequest);
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        return shoppingStoreService.getProduct(productId);
    }
}