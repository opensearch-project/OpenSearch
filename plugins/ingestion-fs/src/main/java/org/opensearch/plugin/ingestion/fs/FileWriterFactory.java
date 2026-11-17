/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IngestionWriter;
import org.opensearch.index.IngestionWriterFactory;

/**
 * Factory for creating file-based ingestion writers.
 */
public class FileWriterFactory implements IngestionWriterFactory {

    /**
     * Initialize a FileWriterFactory for file-based indexing.
     */
    public FileWriterFactory() {}

    @Override
    public IngestionWriter createWriter(IndexMetadata indexMetadata) {
        return new FileIngestionWriter(new FileSourceConfig(indexMetadata.getIngestionSource().getWriterParams()));
    }
}
