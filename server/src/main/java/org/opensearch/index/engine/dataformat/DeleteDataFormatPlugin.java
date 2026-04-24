/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.FormatChecksumStrategy;

/**
 * Plugin interface for providing custom delete data format implementations.
 * Plugins implement this to register their delete data format
 * with the {@link DataFormatRegistry} during node bootstrap.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DeleteDataFormatPlugin extends DataFormatPlugin{

    /**
     * Creates the delete execution engine for the data format. This should be instantiated per shard.
     *
     * @return the delete execution engine instance
     */
    DeleteExecutionEngine<?, ?> deleteEngine();

    /**
     * Not applicable for delete-only plugins. Always throws {@link UnsupportedOperationException}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    default IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig s, FormatChecksumStrategy c) {
        throw new UnsupportedOperationException("Delete plugins do not provide an indexing engine");
    }
}
