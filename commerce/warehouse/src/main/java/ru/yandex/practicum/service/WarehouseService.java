package ru.yandex.practicum.service;

import ru.yandex.practicum.model.*;

import java.util.Map;
import java.util.UUID;

public interface WarehouseService {
    void addNewProduct(NewProductInWarehouseRequest request);

    BookedProductsDto checkProductQuantity(ShoppingCartDto shoppingCartDto);

    void addProductQuantity(AddProductToWarehouseRequest request);

    AddressDto getAddress();

    void shippedDelivery(ShippedToDeliveryRequest shippedToDeliveryRequest);

    void productToWarehouse(Map<UUID,Integer> products);

    BookedProductsDto orderAssembly(AssemblyProductsForOrderRequest assemblyProductsForOrderRequest);
}