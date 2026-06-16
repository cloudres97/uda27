package com.uda.spec.exception;

public class SpecModifyException extends RuntimeException {
    public SpecModifyException(String message) {
        super(message);
    }

    public SpecModifyException(String message, Throwable cause) {
        super(message, cause);
    }
}
