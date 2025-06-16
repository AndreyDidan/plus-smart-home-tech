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
import ru.yandex.practicum.repository.BookingRepository;
import ru.yandex.practicum.repository.WarehouseRepository;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class WarehouseServiceImpi implements WarehouseService {

    private final WarehouseRepository warehouseRepository;
    private final BookingRepository bookingRepository;
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

    @Override
    public void shippedDelivery(ShippedToDeliveryRequest shippedToDeliveryRequest) {
        log.info("Запуск метода shippedDelivery,на входе shippedToDeliveryRequest:{}", shippedToDeliveryRequest);
        Optional<Booking> booking = bookingRepository.findByOrderId(shippedToDeliveryRequest.getOrderId());
        if (booking.isEmpty()) {
            throw new NotFoundException("Заказ с таким orderId:" + shippedToDeliveryRequest.getOrderId() + " не найден");
        }
        Booking updatedBooking = booking.get();
        updatedBooking.setDeliveryId(shippedToDeliveryRequest.getDeliveryId());
        bookingRepository.save(updatedBooking);
    }

    @Override
    @Transactional
    public void productToWarehouse(Map<UUID, Long> products) {
        log.info("Запуск метода productToWarehouse,на входе products:{}", products);
        List<WarehouseProduct> savedProducts = warehouseRepository.findAllById(products.keySet());
        if (savedProducts.size() < products.size()) {
            throw new NotFoundException("Часть товара не найдена складе");
        }
        Set<WarehouseProduct> updatedProducts = new HashSet<>();
        for (WarehouseProduct savedProduct : savedProducts) {
            Long quantity = products.get(savedProduct.getProductId());
            savedProduct.setQuantity(savedProduct.getQuantity() + quantity);
            updatedProducts.add(savedProduct);
        }
        warehouseRepository.saveAll(updatedProducts);
    }

    @Override
    @Transactional
    public BookedProductsDto orderAssembly(AssemblyProductsForOrderRequest assemblyProductsForOrderRequest) {
        log.info("Запуск метода orderAssembly,на входе assemblyProductsForOrderRequest:{}", assemblyProductsForOrderRequest);
        Map<UUID, Long> orderProducts = assemblyProductsForOrderRequest.getProducts();
        Map<UUID, WarehouseProduct> products = getWarehouseProducts(orderProducts.keySet());

        double weight = 0;
        double volume = 0;
        boolean fragile = false;
        for (Map.Entry<UUID, Long> cartProduct : orderProducts.entrySet()) {
            WarehouseProduct product = products.get(cartProduct.getKey());
            long newQuantity = product.getQuantity() - cartProduct.getValue();
            if (newQuantity < 0) {
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "Ошибка, товар из корзины не находится в требуемом количестве на складе");
            }
            product.setQuantity(newQuantity);
            weight += product.getWeight() * cartProduct.getValue();
            volume += product.getHeight() * product.getWeight() * product.getDepth() * cartProduct.getValue();
            fragile = fragile || product.isFragile();
        }
        addBooking(assemblyProductsForOrderRequest);
        saveWarehouseRemains(products.values());

        return new BookedProductsDto(
                weight,
                volume,
                fragile
        );
    }

    private WarehouseProduct getWarehouseProduct(UUID productId) {
        return warehouseRepository.findById(productId).orElseThrow(
                () -> new NoSpecifiedProductInWarehouseException("Нет информации о товаре на складе")
        );
    }

    private Booking getBookingById(UUID orderId) {
        return bookingRepository.findById(orderId).orElseThrow(
                () -> new NotFoundException("Нет информации о бронировании товаров по заказу")
        );
    }

    Map<UUID, WarehouseProduct> getWarehouseProducts(Collection<UUID> ids) {
        Map<UUID, WarehouseProduct> products = warehouseRepository.findAllById(ids)
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, Function.identity()));
        if (products.size() != ids.size()) {
            throw new ProductInShoppingCartLowQuantityInWarehouse("Некоторых товаров нет на складе");
        }

        return products;
    }

    void addBooking(AssemblyProductsForOrderRequest request) {
        Booking booking = Booking.builder()
                .orderId(request.getOrderId())
                .products(request.getProducts())
                .build();
        bookingRepository.save(booking);
    }

    void saveWarehouseRemains(Collection<WarehouseProduct> products) {
        warehouseRepository.saveAll(products);
    }
}