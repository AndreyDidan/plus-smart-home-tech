package ru.yandex.practicum.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "payments")
public class Payment {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "payment_id")
    private UUID paymentId;
    @Column(name = "order_id")
    private UUID orderId;
    @Column(name = "delivery_total")
    private Double deliveryTotal;
    @Column(name = "total_payment")
    private Double totalPayment;
    @Column(name = "fee_total")
    private Double feeTotal;
    @Enumerated(value = EnumType.STRING)
    @Column(name = "payment_state")
    private PaymentState paymentState;
}
