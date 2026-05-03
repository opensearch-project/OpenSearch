/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

/**
 * {@link PublishPhase#INITIALIZED} → {@link PublishPhase#PUBLISHING}. Calls
 * {@link CatalogMetadataClient#startPublishForIndex} off the cluster-state thread.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InitializedProcessor extends AbstractPublishStateProcessor {

    public InitializedProcessor(
        ClusterService clusterService,
        CatalogMetadataClient metadataClient,
        ThreadPool threadPool,
        int maxRetries,
        TimeValue baseBackoff
    ) {
        super(clusterService, metadataClient, threadPool, maxRetries, baseBackoff);
    }

    @Override
    protected PublishPhase expectedPhase() {
        return PublishPhase.INITIALIZED;
    }

    @Override
    public void process(PublishEntry entry) {
        threadPool.generic().execute(() -> runInitialize(entry));
    }

    private void runInitialize(PublishEntry entry) {
        String indexName = entry.indexName();
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
            handleRetryOnError(entry, "index [" + indexName + "] not found in cluster state",
                new IllegalStateException("index metadata missing"));
            return;
        }
        try {
            metadataClient.startPublishForIndex(indexName, indexMetadata);
            moveToNextPhase(entry, PublishPhase.PUBLISHING);
        } catch (Exception e) {
            handleRetryOnError(entry, "startPublishForIndex failed: " + e.getMessage(), e);
        }
    }
}
