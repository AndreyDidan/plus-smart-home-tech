package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "products")
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID productId;

    private String productName;

    private String description;

    private String imageSrc;

    @Enumerated(EnumType.STRING)
    private QuantityState quantityState;

    private Integer rating;

    @Enumerated(EnumType.STRING)
    private ProductState productState;

    @Enumerated(EnumType.STRING)
    private ProductCategory productCategory;

    private Double price;
}
