package ru.yandex.practicum.mapper;

import org.mapstruct.*;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.model.ProductDto;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface ProductMapper {
    ProductDto productToProductDto(Product product);

    Product productDtoToProduct(ProductDto productDto);
}