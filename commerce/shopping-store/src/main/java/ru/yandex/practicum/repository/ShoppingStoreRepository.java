package ru.yandex.practicum.repository;


import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.model.ProductCategory;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShoppingStoreRepository extends JpaRepository<Product, UUID> {
    Optional<Product> findByProductId(UUID productId);
    Page<Product> findByProductCategory(ProductCategory productCategory, Pageable pageable);
}