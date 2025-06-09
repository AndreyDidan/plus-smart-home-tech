package ru.yandex.practicum.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.constant.AddressConstant;
import ru.yandex.practicum.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.mapper.WarehouseMapper;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.repository.WarehouseRepository;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseServiceImpi implements WarehouseService {

    private final WarehouseRepository warehouseRepository;
    private final WarehouseMapper mapper;

    @Transactional
    @Override
    public void addNewProduct(NewProductInWarehouseRequest newProductInWarehouseRequest) {
        log.info("Запуск метода addNewProduct,на входе newProductInWarehouseRequest:{}", newProductInWarehouseRequest);
        if (warehouseRepository.existsById(newProductInWarehouseRequest.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException("Товар productId:" +
                    newProductInWarehouseRequest.getProductId() + " уже есть на складе");
        }
        warehouseRepository.save(mapper.toMap(newProductInWarehouseRequest));
    }

    @Transactional
    @Override
    public BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto) {
        log.info("Запуск метода checkProductQuantity, на входе shoppingCartDto:{}", shoppingCartDto);
        Set<UUID> requestProducts = shoppingCartDto.getProducts().keySet();
        List<WarehouseProduct> products = warehouseRepository.findAllById(requestProducts);
        Set<UUID> foundProductIds = products.stream()
                .map(WarehouseProduct::getProductId)
                .collect(Collectors.toSet());
        Set<UUID> missingProductIds = new HashSet<>(requestProducts);
        missingProductIds.removeAll(foundProductIds);
        if (!missingProductIds.isEmpty()) {
            throw new NotFoundException("Следующие товары не найдены на складе: " + missingProductIds);
        }

        for (WarehouseProduct product : products) {
            if (product.getQuantity() < shoppingCartDto.getProducts().get(product.getProductId())) {
                throw new ProductInShoppingCartLowQuantityInWarehouse("Товара с productId:" + product.getProductId()
                        + " нет в нужном количестве");
            }
        }

        double weightSum = 0;
        double deliveryVolumeSum = 0;
        boolean areThereAnyFragile = false;

        log.info("Перемножаем WarehouseProduct product");
        for (WarehouseProduct product : products) {
            weightSum += product.getWeight() * product.getQuantity();
            deliveryVolumeSum += product.getDepth() * product.getWidth() * product.getHeight() * product.getQuantity();
            if (product.isFragile()) {
                areThereAnyFragile = true;
            }
        }

        log.info("Строим BookedProductsDto");
        return BookedProductsDto.builder()
                .deliveryWeight(weightSum)
                .fragile(areThereAnyFragile)
                .deliveryVolume(deliveryVolumeSum)
                .build();
    }

    @Override
    @Transactional
    public void addProductQuantity(AddProductToWarehouseRequest addProductToWarehouseRequest) {
        log.info("Запуск метода addProductQuantity,на входе addProductToWarehouseRequest:{}", addProductToWarehouseRequest);
        if (!warehouseRepository.existsById(addProductToWarehouseRequest.getProductId())) {
            throw new NoSpecifiedProductInWarehouseException("Товар с productId:" + addProductToWarehouseRequest
                    .getProductId() + " не найден");
        }

        WarehouseProduct product = warehouseRepository.findById(addProductToWarehouseRequest.getProductId()).orElseThrow(
                () -> new NoSpecifiedProductInWarehouseException("Информация о товаре "
                        + addProductToWarehouseRequest.getProductId() + " не найдена.")
        );
        Long currentQty = product.getQuantity() != null ? product.getQuantity() : 0L;
        product.setQuantity(currentQty + addProductToWarehouseRequest.getQuantity());
        warehouseRepository.save(product);
    }

    @Override
    public AddressDto getAddress() {
        String defValue = AddressConstant.getAddress();
        return new AddressDto(
                defValue,
                defValue,
                defValue,
                defValue,
                defValue
        );
    }
}