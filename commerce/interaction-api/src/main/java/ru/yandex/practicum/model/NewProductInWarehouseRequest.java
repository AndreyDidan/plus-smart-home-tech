package ru.yandex.practicum.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NewProductInWarehouseRequest {
    @NotNull
    private UUID productId;

    private Boolean fragile;

    @NotNull
    private DimensionDto dimension;

    @DecimalMin(value = "1")
    private Double weight;
}
