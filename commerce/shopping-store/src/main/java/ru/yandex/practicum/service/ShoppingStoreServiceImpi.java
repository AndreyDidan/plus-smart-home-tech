package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.ShoppingStoreRepository;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShoppingStoreServiceImpi implements ShoppingStoreService {
    private final ShoppingStoreRepository shoppingStoreRepository;
    private final ProductMapper mapper;

    @Override
    @Transactional
    public Page<ProductDto> getProductPage(ProductCategory productCategory, Pageable pageable) {
        log.info("Запуск метода getProductPage, на входе productCategory :{}, pageable {}", productCategory, pageable);
        int page = pageable.getPage() != null ? pageable.getPage() : 0;
        int size = pageable.getSize() != null ? pageable.getSize() : 10;

        Sort sort = Sort.unsorted();

        if (pageable.getSort() != null && !pageable.getSort().isEmpty()) {
            List<Sort.Order> orders = pageable.getSort().stream()
                    .map(sortParam -> {
                        String[] parts = sortParam.split(":");
                        String field = parts[0];
                        boolean isDesc = parts.length > 1 && parts[1].equalsIgnoreCase("desc");
                        return new Sort.Order(isDesc ? Sort.Direction.DESC : Sort.Direction.ASC, field);
                    })
                    .toList();
            sort = Sort.by(orders);
        } else {
            sort = Sort.by("productName").ascending();
        }

        PageRequest pageRequest = PageRequest.of(page, size, sort);
        Page<Product> pageResult = shoppingStoreRepository.findByProductCategory(productCategory, pageRequest);

        List<ProductDto> dtos = pageResult.stream()
                .map(mapper::productToProductDto)
                .toList();

        return new PageImpl<>(dtos, pageRequest, pageResult.getTotalElements());
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        log.info("Запуск метода getProduct, на входе productId :{}", productId);
        Product product = shoppingStoreRepository.findByProductId(productId).orElseThrow(
                () -> new NotFoundException("Товар с id " + productId + " не найден"));
        return mapper.productToProductDto(product);
    }

    @Override
    @Transactional
    public ProductDto addProduct(ProductDto productDto) {
        log.info("Запуск метода addProduct, на входе productDto :{}", productDto);
        Product newProduct = mapper.productDtoToProduct(productDto);
        newProduct = shoppingStoreRepository.save(newProduct);
        return mapper.productToProductDto(newProduct);
    }

    @Override
    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.info("Запуск метода updateProduct, на входе productDto :{}", productDto);
        if (!shoppingStoreRepository.existsById(productDto.getProductId())) {
            throw new NotFoundException("Товар с id " + productDto.getProductId() + " не найден");
        }
        return mapper.productToProductDto(shoppingStoreRepository.save(mapper.productDtoToProduct(productDto)));
    }

    @Override
    @Transactional
    public void deleteProduct(UUID productId) {
        log.info("Запуск метода deleteProduct, на входе productId :{}", productId);
        Product product = shoppingStoreRepository.findById(productId)
                .orElseThrow(() -> new NotFoundException("Товар с id " + productId + " не найден"));

        product.setProductState(ProductState.DEACTIVATE);
        shoppingStoreRepository.save(product);
    }

    @Transactional
    @Override
    public boolean setProductQuantityState(SetProductQuantityStateRequest setProductQuantityStateRequest) {
        log.info("Запуск метода setProductQuantityState, на входе productId :{}", setProductQuantityStateRequest);
        return shoppingStoreRepository.findById(setProductQuantityStateRequest.getProductId())
                .map(product -> {
                    product.setQuantityState(setProductQuantityStateRequest.getQuantityState());
                    shoppingStoreRepository.save(product);
                    return true;
                })
                .orElseThrow(() -> new NotFoundException("Товар с id " + setProductQuantityStateRequest.getProductId()
                        + " не найден"));
    }
}