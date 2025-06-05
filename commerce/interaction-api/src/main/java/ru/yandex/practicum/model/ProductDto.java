package ru.yandex.practicum.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.*;

import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProductDto {

    private UUID productId;

    @NotNull
    private String productName;

    @NotNull
    private String description;

    private String imageSrc;

    @NotNull
    private QuantityState quantityState;

    @NotNull
    private ProductState productState;

    @NotNull
    private ProductCategory productCategory;

    @Min(value = 1)
    @Max(value = 5)
    private Integer rating;

    @PositiveOrZero
    private Double price;
}
