package io.techasylum.kafka.statestore.document.internals;

/**
 * Like a {@link Runnable} that allows exceptions to be thrown or a {@link java.util.concurrent.Callable}
 * that does not return a value.
 */
public interface ValuelessCallable {
    void call() throws Exception;
}
