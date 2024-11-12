/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.ingest;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionSourceConfig;

import java.io.Closeable;
import java.io.IOException;

/**
 * IngestionManager is responsible for managing the ingestion of data for a shard
 */
public class IngestionManager implements Closeable {

    private final IngestionConsumerFactory consumerFactory;

    public IngestionManager(
                                IngestionConsumerFactory   consumerFactory,
                                int shard) {
        this.consumerFactory = consumerFactory;
    }



    @Override
    public void close() throws IOException {

    }
}
