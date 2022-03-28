package io.techasylum.kafka.statestore.document.no2;

import org.dizitart.no2.NitriteBuilder;

/**
 * Callback interface that can be used to configure {@link NitriteBuilder} directly.
 */
@FunctionalInterface
public interface NitriteCustomizer {

    /**
     *
     * @param builder the builder used to create a new Nitrite database
     */
    NitriteBuilder customize(final NitriteBuilder builder);

}
