package io.techasylum.kafka.statestore.document.internals;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;

/**
 * A clock that you can manually advance by calling sleep
 */
public class MockTime implements Time {

    public interface Listener {
        void onTimeUpdated();
    }

    /**
     * Listeners which are waiting for time changes.
     */
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private final long autoTickMs;

    // Values from `nanoTime` and `currentTimeMillis` are not comparable, so we store them separately to allow tests
    // using this class to detect bugs where this is incorrectly assumed to be true
    private final AtomicLong timeMs;
    private final AtomicLong highResTimeNs;

    public MockTime() {
        this(0);
    }

    public MockTime(long autoTickMs) {
        this(autoTickMs, System.currentTimeMillis(), System.nanoTime());
    }

    public MockTime(long autoTickMs, long currentTimeMs, long currentHighResTimeNs) {
        this.timeMs = new AtomicLong(currentTimeMs);
        this.highResTimeNs = new AtomicLong(currentHighResTimeNs);
        this.autoTickMs = autoTickMs;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public long milliseconds() {
        maybeSleep(autoTickMs);
        return timeMs.get();
    }

    @Override
    public long nanoseconds() {
        maybeSleep(autoTickMs);
        return highResTimeNs.get();
    }

    private void maybeSleep(long ms) {
        if (ms != 0)
            sleep(ms);
    }

    @Override
    public void sleep(long ms) {
        timeMs.addAndGet(ms);
        highResTimeNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
        tick();
    }

    @Override
    public void waitObject(Object obj, Supplier<Boolean> condition, long deadlineMs) throws InterruptedException {
        Listener listener = () -> {
            synchronized (obj) {
                obj.notify();
            }
        };
        listeners.add(listener);
        try {
            synchronized (obj) {
                while (milliseconds() < deadlineMs && !condition.get()) {
                    obj.wait();
                }
                if (!condition.get())
                    throw new TimeoutException("Condition not satisfied before deadline");
            }
        } finally {
            listeners.remove(listener);
        }
    }

    public void setCurrentTimeMs(long newMs) {
        long oldMs = timeMs.getAndSet(newMs);

        // does not allow to set to an older timestamp
        if (oldMs > newMs)
            throw new IllegalArgumentException("Setting the time to " + newMs + " while current time " + oldMs + " is newer; this is not allowed");

        highResTimeNs.set(TimeUnit.MILLISECONDS.toNanos(newMs));
        tick();
    }

    private void tick() {
        for (Listener listener : listeners) {
            listener.onTimeUpdated();
        }
    }
}
