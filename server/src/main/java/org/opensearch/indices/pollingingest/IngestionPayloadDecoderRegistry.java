/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionPayloadDecoderFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Node-scoped registry of named {@link IngestionPayloadDecoderFactory} instances.
 *
 * <p>Core registers built-in decoders (e.g. {@code xcontent}) first. Plugin-provided
 * factories are added afterwards. Registering the same name twice fails fast at node startup.
 */
public class IngestionPayloadDecoderRegistry {

    private final Map<String, IngestionPayloadDecoderFactory> factories;

    private IngestionPayloadDecoderRegistry(Map<String, IngestionPayloadDecoderFactory> factories) {
        this.factories = Collections.unmodifiableMap(factories);
    }

    /**
     * Returns the factory registered under {@code decoderType}.
     *
     * @throws IllegalArgumentException if no factory is registered for the given type
     */
    public IngestionPayloadDecoderFactory get(String decoderType) {
        IngestionPayloadDecoderFactory factory = factories.get(decoderType);
        if (factory == null) {
            throw new IllegalArgumentException(
                "No ingestion payload decoder registered for type [" + decoderType + "]. " + "Available types: " + factories.keySet()
            );
        }
        return factory;
    }

    /** Returns all registered factories, for lifecycle management (e.g. closing on node shutdown). */
    public Map<String, IngestionPayloadDecoderFactory> factories() {
        return factories;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builds the registry. Core decoders should be registered before plugin decoders.
     * Duplicate names cause an {@link IllegalStateException} at build time.
     */
    public static class Builder {
        private final Map<String, IngestionPayloadDecoderFactory> factories = new HashMap<>();

        public Builder register(String name, IngestionPayloadDecoderFactory factory) {
            if (factories.containsKey(name)) {
                throw new IllegalStateException("Duplicate ingestion payload decoder registration for name [" + name + "]");
            }
            factories.put(name, factory);
            return this;
        }

        public IngestionPayloadDecoderRegistry build() {
            return new IngestionPayloadDecoderRegistry(factories);
        }
    }
}
