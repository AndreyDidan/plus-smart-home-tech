package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "deliveryes")
public class Delivery {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "delivery_id")
    private UUID deliveryId;
    @Column(name = "order_id")
    private UUID orderId;
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "from_address_id")
    private Address fromAddress;
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "to_address_id")
    private Address toAddress;
    @Enumerated(value = EnumType.STRING)
    @Column(name = "state")
    private DeliveryState deliveryState;
}
