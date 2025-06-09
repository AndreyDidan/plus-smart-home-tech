package ru.yandex.practicum.exception;

import feign.FeignException;
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
        log.warn("404 Not Found: {}", e.getMessage(), e);
        return createError(e, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({ValidationException.class,
            SpecifiedProductAlreadyInWarehouseException.class,
            ProductInShoppingCartLowQuantityInWarehouse.class,
            NoSpecifiedProductInWarehouseException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Error handleBadRequest(Exception e) {
        log.warn("400 Bad Request: {}", e.getMessage(), e);
        return createError(e, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(FeignException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Error handleFeignException(FeignException e) {
        log.error("Feign error: {}", e.getMessage(), e);
        return createError(e, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Error handleGeneralException(Exception e) {
        log.error("500 Internal Server Error: {}", e.getMessage(), e);
        return createError(e, HttpStatus.INTERNAL_SERVER_ERROR);
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