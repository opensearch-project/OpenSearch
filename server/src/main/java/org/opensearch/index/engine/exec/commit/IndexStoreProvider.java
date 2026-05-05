/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.store.Store;

/**
 * Provides access to the shard's {@link Store} for opening NRT readers
 * or incorporating segments.
 * <p>
 * Implemented by {@link org.opensearch.index.engine.dataformat.IndexingExecutionEngine}
 * instances that own or wrap a shard-level store. The provider is passed to
 * {@link org.opensearch.plugins.SearchBackEndPlugin#createReaderManager} so that
 * search backends can open readers without depending on engine internals.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexStoreProvider {

    /**
     * Returns the store for the current shard.
     *
     * @return the store, or null if not available
     */
    FormatStore getStore(DataFormat dataFormat);

    /**
     * A format-specific store wrapper providing access to the underlying {@link Store}.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    interface FormatStore {
        Store store();
    }
}
