package com.fs.iquant.wind_fetcher.exceptions;

public class TdbGetDataException extends Exception{
    private static final long serialVersionUID = 1L;

    public TdbGetDataException() {
    }

    public TdbGetDataException(String message) {
        super(message);
    }

    public TdbGetDataException(Throwable cause) {
        super(cause);
    }

    public TdbGetDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
