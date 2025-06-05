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

import java.util.List;
import java.util.Set;
import java.util.UUID;

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
        log.info("Запуск метода checkProductQuantity,на входе shoppingCartDto:{}", shoppingCartDto);
        Set<UUID> requestProducts = shoppingCartDto.getProducts().keySet();
        List<WarehouseProduct> products = warehouseRepository.findAllById(requestProducts);

        if (products.size() < requestProducts.size()) {
            throw new NotFoundException("Часть товара не найдена складе");
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

        for (WarehouseProduct product : products) {
            weightSum += product.getWeight() * product.getQuantity();
            deliveryVolumeSum += product.getWeight() * product.getDepth() * product.getWidth() * product.getQuantity();
            if (product.isFragile()) {
                areThereAnyFragile = true;
            }
        }

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

        WarehouseProduct product = warehouseRepository.findById(addProductToWarehouseRequest.getProductId()).get();
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
