package io.techasylum.kafka.statestore.document.internals;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StreamThreadStateStoreProvider;

public class StateStoreProviderStub extends StreamThreadStateStoreProvider {

    //<store name : partition> -> state store
    private final Map<Entry<String, Integer>, StateStore> stores = new HashMap<>();
    private final boolean throwException;

    private final int defaultStorePartition = 0;

    public StateStoreProviderStub(final boolean throwException) {
        super(null);
        this.throwException = throwException;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> stores(final StoreQueryParameters storeQueryParameters) {
        final String storeName = storeQueryParameters.storeName();
        final QueryableStoreType<T> queryableStoreType = storeQueryParameters.queryableStoreType();
        if (throwException) {
            throw new InvalidStateStoreException("store is unavailable");
        }
        if (storeQueryParameters.partition() != null) {
            final Entry<String, Integer> stateStoreKey = new SimpleEntry<>(storeName, storeQueryParameters.partition());
            if (stores.containsKey(stateStoreKey) && queryableStoreType.accepts(stores.get(stateStoreKey))) {
                return (List<T>) Collections.singletonList(stores.get(stateStoreKey));
            }
            return Collections.emptyList();
        }
        return (List<T>) Collections.unmodifiableList(
                stores.entrySet().stream().
                        filter(entry -> entry.getKey().getKey().equals(storeName) && queryableStoreType.accepts(entry.getValue())).
                        map(Entry::getValue).
                        collect(Collectors.toList()));
    }

    public void addStore(final String storeName,
                         final StateStore store) {
        addStore(storeName, defaultStorePartition, store);
    }

    public void addStore(final String storeName,
                         final int partition,
                         final StateStore store) {
        stores.put(new SimpleEntry<>(storeName, partition), store);
    }
}