package io.techasylum.kafka.statestore.document.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StateManager;
import org.apache.kafka.streams.processor.internals.Task.TaskType;

import java.io.File;
import java.util.Map;

public class StateManagerStub implements StateManager {

    @Override
    public File baseDir() {
        return null;
    }

    @Override
    public void registerStore(final StateStore store,
                              final StateRestoreCallback stateRestoreCallback) {}

    @Override
    public void flush() {}

    @Override
    public void close() {}

    @Override
    public StateStore getStore(final String name) {
        return null;
    }

    @Override
    public StateStore getGlobalStore(final String name) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> changelogOffsets() {
        return null;
    }

    @Override
    public void updateChangelogOffsets(final Map<TopicPartition, Long> writtenOffsets) {}

    @Override
    public void checkpoint() {}

    @Override
    public TaskType taskType() {
        return null;
    }

    @Override
    public String changelogFor(final String storeName) {
        return null;
    }
}