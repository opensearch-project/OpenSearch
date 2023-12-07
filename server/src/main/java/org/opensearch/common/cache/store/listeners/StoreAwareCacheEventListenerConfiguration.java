/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store.listeners;

import java.util.EnumSet;
import java.util.Objects;

public class StoreAwareCacheEventListenerConfiguration<K, V> {

    private final EnumSet<EventType> eventTypes;
    private final StoreAwareCacheEventListener<K, V> eventListener;

    public StoreAwareCacheEventListenerConfiguration(Builder<K, V> builder) {
        this.eventListener = Objects.requireNonNull(builder.eventListener);
        this.eventTypes = Objects.requireNonNull(builder.eventTypes);
    }

    public EnumSet<EventType> getEventTypes() {
        return eventTypes;
    }

    public StoreAwareCacheEventListener<K, V> getEventListener() {
        return eventListener;
    }

    public static class Builder<K, V> {
        private EnumSet<EventType> eventTypes;
        private StoreAwareCacheEventListener<K, V> eventListener;

        public Builder() {}

        public Builder<K, V> setEventTypes(EnumSet<EventType> eventTypes) {
            this.eventTypes = eventTypes;
            return this;
        }

        public Builder<K, V> setEventListener(StoreAwareCacheEventListener<K, V> eventListener) {
            this.eventListener = eventListener;
            return this;
        }

        public StoreAwareCacheEventListenerConfiguration<K, V> build() {
            return new StoreAwareCacheEventListenerConfiguration<>(this);
        }
    }
}
