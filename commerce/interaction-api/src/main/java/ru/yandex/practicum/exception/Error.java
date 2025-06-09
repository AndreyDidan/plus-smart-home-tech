package ru.yandex.practicum.exception;

import lombok.Builder;
import lombok.Data;
import org.springframework.http.HttpStatus;

@Data
@Builder
public class Error {
    private Throwable cause;
    private StackTraceElement[] stackTrace;
    private HttpStatus httpstatus;
    private String userMessage;
    private String message;
    private Throwable[] suppressed;
    private String localizedMessage;
}