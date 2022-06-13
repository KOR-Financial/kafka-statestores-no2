package io.techasylum.kafka.statestore.document.internals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.InvalidStateStorePartitionException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.apache.kafka.streams.state.internals.StreamThreadStateStoreProvider;

/**
 * Provides a wrapper over multiple underlying {@link StateStoreProvider}s
 */
public class WrappingStoreProvider implements StateStoreProvider {

    private final Collection<StreamThreadStateStoreProvider> storeProviders;
    private StoreQueryParameters storeQueryParameters;

    public WrappingStoreProvider(final Collection<StreamThreadStateStoreProvider> storeProviders,
                          final StoreQueryParameters storeQueryParameters) {
        this.storeProviders = storeProviders;
        this.storeQueryParameters = storeQueryParameters;
    }

    //visible for testing
    public void setStoreQueryParameters(final StoreQueryParameters storeQueryParameters) {
        this.storeQueryParameters = storeQueryParameters;
    }

    @Override
    public <T> List<T> stores(final String storeName,
                              final QueryableStoreType<T> queryableStoreType) {
        final List<T> allStores = new ArrayList<>();
        for (final StreamThreadStateStoreProvider storeProvider : storeProviders) {
            final List<T> stores = storeProvider.stores(storeQueryParameters);
            if (!stores.isEmpty()) {
                allStores.addAll(stores);
                if (storeQueryParameters.partition() != null) {
                    break;
                }
            }
        }
        if (allStores.isEmpty()) {
            if (storeQueryParameters.partition() != null) {
                throw new InvalidStateStorePartitionException(
                        String.format("The specified partition %d for store %s does not exist.",
                                storeQueryParameters.partition(),
                                storeName));
            }
            throw new InvalidStateStoreException("The state store, " + storeName + ", may have migrated to another instance.");
        }
        return allStores;
    }
}