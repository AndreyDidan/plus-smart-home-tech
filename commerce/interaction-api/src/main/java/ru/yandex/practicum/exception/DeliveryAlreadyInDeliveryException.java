package ru.yandex.practicum.exception;

public class DeliveryAlreadyInDeliveryException extends RuntimeException {
    public DeliveryAlreadyInDeliveryException(String message) {
        super(message);
    }
}