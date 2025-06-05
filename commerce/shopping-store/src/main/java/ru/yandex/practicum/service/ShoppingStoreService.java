package ru.yandex.practicum.service;

import org.springframework.data.domain.Page;
import ru.yandex.practicum.model.Pageable;
import ru.yandex.practicum.model.ProductCategory;
import ru.yandex.practicum.model.ProductDto;
import ru.yandex.practicum.model.SetProductQuantityStateRequest;

import java.util.UUID;

public interface ShoppingStoreService {

    Page<ProductDto> getProductPage(ProductCategory category, Pageable pageable);

    ProductDto getProduct(UUID productId);

    ProductDto addProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    boolean deleteProduct(UUID productId);

    boolean setProductQuantityState(SetProductQuantityStateRequest setProductQuantityStateRequest);
}
