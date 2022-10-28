package com.droidhang.clickhouse.exception;

public class UnsupportedDataTypeException extends Exception{

    public UnsupportedDataTypeException(String message) {
        super(message);
    }

    public UnsupportedDataTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
