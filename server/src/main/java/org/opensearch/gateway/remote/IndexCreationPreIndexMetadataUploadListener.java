/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.IndexMetadata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Hook for running code that needs to be executed before the upload of index metadata during index creation or
 * after enabling the remote cluster statement for the first time. The listener is intended to be run in parallel and
 * async with the index metadata upload.
 *
 * @opensearch.internal
 */
public interface IndexCreationPreIndexMetadataUploadListener {

    /**
     * This returns the additional count that needs to be added in the latch present in {@link RemoteClusterStateService}
     * which is used to achieve parallelism and async nature of uploads for index metadata upload. The same latch is used
     * for running pre index metadata upload listener.
     *
     * @param newIndexMetadataList list of index metadata of new indexes (or first time index metadata upload).
     * @return latch count to be added by the caller.
     */
    int latchCount(List<IndexMetadata> newIndexMetadataList);

    /**
     * This will run the pre index metadata upload listener using the {@code newIndexMetadataList}, {@code latch} and
     * {@code exceptionList}. This method must execute the operation in parallel and async to ensure that the cluster state
     * upload time remains the same.
     *
     * @param newIndexMetadataList list of index metadata of new indexes (or first time index metadata upload).
     * @param latch this is used for counting down once the unit of work per index is done.
     * @param exceptionList exception if any during run will be added here and used by the caller.
     */
    void run(List<IndexMetadata> newIndexMetadataList, CountDownLatch latch, List<Exception> exceptionList) throws IOException;
}
