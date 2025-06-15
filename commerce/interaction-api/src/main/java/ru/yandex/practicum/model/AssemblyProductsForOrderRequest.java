package ru.yandex.practicum.model;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AssemblyProductsForOrderRequest {
    @NotNull
    private Map<UUID, Long> products;

    @NotNull
    private UUID orderId;
}
