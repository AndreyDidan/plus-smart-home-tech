package ru.yandex.practicum.exception;

import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Error handleNotFoundException(NotFoundException e) {
        return createError(e, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({ValidationException.class, ValidationException.class,
            SpecifiedProductAlreadyInWarehouseException.class,
            ProductInShoppingCartLowQuantityInWarehouse.class, NoSpecifiedProductInWarehouseException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Error handleBadRequest(Exception e) {
        return createError(e, HttpStatus.BAD_REQUEST);
    }

    private Error createError(Exception e, HttpStatus httpstatus) {
        return Error.builder()
                .cause(e.getCause())
                .stackTrace(e.getStackTrace())
                .httpstatus(httpstatus)
                .userMessage(e.getMessage())
                .message(e.getMessage())
                .suppressed(e.getSuppressed())
                .localizedMessage(e.getLocalizedMessage())
                .build();
    }
}