package io.techasylum.kafka.statestore.document.internals;

/**
 * This class can be used in the callback given to {@link TestUtils#retryOnExceptionWithTimeout(long, long, ValuelessCallable)}
 * to indicate that a particular exception should not be retried. Instead the retry operation will
 * be aborted immediately and the exception will be rethrown.
 */
public class NoRetryException extends RuntimeException {
    private final Throwable cause;

    public NoRetryException(Throwable cause) {
        this.cause = cause;
    }

    @Override
    public Throwable getCause() {
        return this.cause;
    }
}