package ru.yandex.practicum.model;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
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
public class OrderDto {

    @NotNull
    private UUID orderId;

    @NotNull
    private UUID shoppingCartId;

    @NotNull
    Map<UUID, @Positive Long> products;

    @NotNull
    private UUID paymentId;

    @NotNull
    private UUID deliveryId;

    private OrderState orderState;

    private Double deliveryWeight;

    private Double deliveryVolume;

    private Boolean fragile;

    private Double totalPrice;

    private Double deliveryPrice;

    private Double productPrice;
}
