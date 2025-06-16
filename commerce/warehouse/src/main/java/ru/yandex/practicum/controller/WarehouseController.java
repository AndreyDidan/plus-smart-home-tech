package ru.yandex.practicum.controller;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.feign.client.WarehouseClient;
import ru.yandex.practicum.model.*;
import ru.yandex.practicum.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/warehouse")
@AllArgsConstructor
public class WarehouseController implements WarehouseClient {
    private final WarehouseService service;

    @Override
    public void addNewProduct(NewProductInWarehouseRequest request) {
        service.addNewProduct(request);
    }

    @Override
    public BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto) {
        return service.checkProductQuantity(shoppingCartDto);
    }

    @Override
    public void addProduct(AddProductToWarehouseRequest request) {
        service.addProductQuantity(request);
    }

    @Override
    public AddressDto getAddress() {
        return service.getAddress();
    }

    @Override
    public void shippedDelivery(ShippedToDeliveryRequest shippedToDeliveryRequest) {
        service.shippedDelivery(shippedToDeliveryRequest);
    }

    @Override
    public void productToWarehouse(Map<UUID, Long> products) {
        service.productToWarehouse(products);
    }

    @Override
    public BookedProductsDto orderAssembly(AssemblyProductsForOrderRequest assemblyProductsForOrderRequest) {
        return service.orderAssembly(assemblyProductsForOrderRequest);
    }
}