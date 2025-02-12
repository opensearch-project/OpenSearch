/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

/**
 * Transport specific adapter providers which could be injected into the transport processing chain. The transport adapters
 * are transport specific and do not have any common abstraction on top.
 * @param <T> transport type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TransportAdapterProvider<T> {
    /**
     * The name of this transport adapter provider (and essentially are freestyle).
     * @return the name of this transport adapter provider
     */
    String name();

    /**
     * Provides a new transport adapter of required transport adapter class and transport instance.
     * @param <C> transport adapter class
     * @param settings settings
     * @param transport HTTP transport instance
     * @param adapterClass required transport adapter class
     * @return the non-empty {@link Optional} if the transport adapter could be created, empty one otherwise
     */
    <C> Optional<C> create(Settings settings, T transport, Class<C> adapterClass);
}
