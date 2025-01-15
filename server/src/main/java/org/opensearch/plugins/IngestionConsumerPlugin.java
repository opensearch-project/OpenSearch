/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IngestionConsumerFactory;

import java.util.Map;

/**
 * An extension point for {@link Plugin} implementations to add custom ingestion consumers for the {@link org.opensearch.index.engine.IngestionEngine}
 *
 * @opensearch.api
 */
@ExperimentalApi
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
}
