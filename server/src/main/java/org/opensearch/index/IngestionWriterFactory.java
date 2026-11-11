/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Creates the {@link IngestionWriter} that produces a write-enabled pull-based index's operations to its stream.
 *
 * <p>A factory is registered at node startup (via
 * {@link org.opensearch.plugins.IngestionConsumerPlugin#getIngestionWriterFactories()}) by the plugin that provides
 * the consumer for the same source type, so one plugin owns both the reading and the writing side of the stream.
 */
@ExperimentalApi
public interface IngestionWriterFactory {

    /**
     * Creates a writer for an index, configured from its {@code index.ingestion_source.*} settings.
     *
     * @param indexMetadata the metadata of the write-enabled index
     * @return the writer
     */
    IngestionWriter createWriter(IndexMetadata indexMetadata);
}
