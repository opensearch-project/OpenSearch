/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.spi;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service Provider Interface for DataFusion data source codecs.
 * Implementations provide access to different data formats (CSV, Parquet, etc.)
 * through the DataFusion query engine.
 */
public interface DataSourceCodec {

    /**
     * Register a directory containing data files with the runtime environment to prewarm cache
     * This ideally should be used as part of each refresh - equivalent of acquire searcher
     * where we register the files associated with this particular refresh point
     */
    CompletableFuture<Void> registerDirectory(String directoryPath, List<String> fileNames, long runtimeId);

    /**
     * Create a new session context for query execution.
     *
     * @param globalRuntimeEnvId the global runtime environment ID
     * @return a CompletableFuture containing the session context ID
     */
    CompletableFuture<Long> createSessionContext(long globalRuntimeEnvId);

    /**
     * Execute a Substrait query plan.
     *
     * @param sessionContextId the session context ID
     * @param substraitPlanBytes the serialized Substrait query plan
     * @return a CompletableFuture containing the result stream
     */
    CompletableFuture<RecordBatchStream> executeSubstraitQuery(long sessionContextId, byte[] substraitPlanBytes);

    /**
     * Close a session context and free associated resources.
     *
     * @param sessionContextId the session context ID to close
     * @return a CompletableFuture that completes when the context is closed
     */
    CompletableFuture<Void> closeSessionContext(long sessionContextId);
}
