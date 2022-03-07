package io.techasylum.kafka.statestore.document.internals;

/**
 * Interface to wrap actions that are required to wait until a condition is met
 * for testing purposes.  Note that this is not intended to do any assertions.
 */
@FunctionalInterface
public interface TestCondition {

    boolean conditionMet() throws Exception;
}
