package com.taboola.api;

import com.taboola.api.exceptions.BadRequestException;
import com.taboola.api.exceptions.NotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class CounterControllerAdvice {

    @ExceptionHandler(NotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorMessage notFoundHandler(NotFoundException ex) {
        return new ErrorMessage(ex.getMessage());
    }

    @ExceptionHandler(BadRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorMessage badRequestHandler(BadRequestException ex) {
        return new ErrorMessage(ex.getMessage());
    }

    private static class ErrorMessage{
        private final String error;

        private ErrorMessage(String errorMessage) {
            this.error = errorMessage;
        }

        public String getError() {
            return error;
        }
    }
}
