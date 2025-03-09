package com.esamtrade.bucketbase;

public class DeleteError extends Exception {
    public DeleteError(String message) {
        super(message);
    }

    public DeleteError(String message, Throwable cause) {
        super(message, cause);
    }

    public DeleteError(Throwable cause) {
        super(cause);
    }

    public DeleteError() {
        super();
    }
}
