package com.fs.iquant.wind_fetcher.exceptions;

public class TdbConnectionException extends RuntimeException {
    private static final long serialVersionUID = 2L;

    public TdbConnectionException() {
    }

    public TdbConnectionException(String message) {
        super(message);
    }

    public TdbConnectionException(Throwable cause) {
        super(cause);
    }

    public TdbConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
