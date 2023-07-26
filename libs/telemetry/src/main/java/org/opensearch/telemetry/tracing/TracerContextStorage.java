/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Storage interface used for storing tracing context
 * @param <K> key type
 * @param <V> value type
 *
 * @opensearch.internal
 */
public interface TracerContextStorage<K, V> {
    /**
     * Key for storing current span
     */
    String CURRENT_SPAN = "current_span";

    /**
     * Fetches value corresponding to key
     * @param key of the tracing context
     * @return value for key
     */
    V get(K key);

    /**
     * Puts tracing context value with key
     * @param key of the tracing context
     * @param value of the tracing context
     */
    void put(K key, V value);
}
