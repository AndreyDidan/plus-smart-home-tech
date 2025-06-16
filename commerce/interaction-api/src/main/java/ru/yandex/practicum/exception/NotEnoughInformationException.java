package ru.yandex.practicum.exception;

public class NotEnoughInformationException extends RuntimeException {
    public NotEnoughInformationException(String message) {
        super(message);
    }
}