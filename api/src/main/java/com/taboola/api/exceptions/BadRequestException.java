package com.taboola.api.exceptions;

public class BadRequestException extends RuntimeException {
    public BadRequestException(final String msg){
        super(msg);
    }
}
