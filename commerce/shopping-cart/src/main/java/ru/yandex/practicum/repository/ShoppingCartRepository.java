package ru.yandex.practicum.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.model.Cart;

import java.util.Optional;

@Repository
public interface ShoppingCartRepository extends JpaRepository<Cart, Long> {
    Optional<Cart> findByUsername(String username);
}