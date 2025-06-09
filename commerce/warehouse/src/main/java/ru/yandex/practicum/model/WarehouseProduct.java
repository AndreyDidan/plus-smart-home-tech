package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "warehouse_products")
public class WarehouseProduct {
    @Id
    @Column(name = "product_id")
    private UUID productId;

    private boolean fragile;

    private Double width;

    private Double height;

    private Double depth;

    private double weight;

    @Column(name = "quantity", nullable = false)
    @Builder.Default
    private Long quantity = 0L;
}