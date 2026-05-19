/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

/**
 * Each backend (DataFusion, Parquet, future engines) implements this interface
 * to provide its stats to the Mustang Stats Framework. The Analytics Plugin
 * discovers {@code BackendStatsProvider} implementations and iterates over them
 * to collect stats from all registered backends.
 */
public interface BackendStatsProvider {

    /**
     * Returns the backend's identifier, e.g. {@code "datafusion"}, {@code "parquet"}.
     *
     * @return a non-null backend name
     */
    String name();

    /**
     * Returns the backend's stats object.
     *
     * @return a non-null {@link PluginStats} instance
     */
    PluginStats getBackendStats();
}
