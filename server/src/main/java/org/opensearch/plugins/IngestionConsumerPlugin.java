/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionPayloadDecoderFactory;
import org.opensearch.index.IngestionWriterFactory;

import java.util.Map;

/**
 * An extension point for {@link Plugin} implementations to add custom ingestion consumers for the {@link org.opensearch.index.engine.IngestionEngine}
 *
 * @opensearch.api
 */
@PublicApi(since = "3.6.0")
public interface IngestionConsumerPlugin {

    /**
     * When an ingestion index is created this method is invoked for each ingestion consumer plugin.
     * Ingestion consumer plugins can inspect the index settings to determine which ingestion consumer to provide.
     *
     * @return a map from the ingestion consumer type to the factory
     */
    Map<String, IngestionConsumerFactory> getIngestionConsumerFactories();

    /**
     * @return the type of the ingestion consumer plugin. the type name shall be in upper case
     */
    String getType();

    /**
     * Returns a map of decoder-name → factory contributed by this plugin.
     *
     * <p>Decoder names must be globally unique across all installed plugins and core built-ins.
     * Node startup fails with a duplicate-registration error if two sources register the same name.
     *
     * @return map of decoder names to their factories; empty map if this plugin provides no decoders
     */
    default Map<String, IngestionPayloadDecoderFactory> getIngestionPayloadDecoderFactories() {
        return Map.of();
    }

    /**
     * Returns a map of source type → writer factory, for the source types this plugin also accepts writes for.
     *
     * @return map of source types to their writer factories; empty map if this plugin's sources are read-only
     */
    @ExperimentalApi
    default Map<String, IngestionWriterFactory> getIngestionWriterFactories() {
        return Map.of();
    }
}
