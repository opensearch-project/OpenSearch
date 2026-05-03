/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

/**
 * {@link PublishPhase#FINALIZING_SUCCESS} → entry removed. Calls
 * {@link CatalogMetadataClient#finalizePublish}{@code (indexName, true)}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FinalizingSuccessProcessor extends AbstractPublishStateProcessor {

    public FinalizingSuccessProcessor(
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
        return PublishPhase.FINALIZING_SUCCESS;
    }

    @Override
    public void process(PublishEntry entry) {
        threadPool.generic().execute(() -> runFinalize(entry));
    }

    private void runFinalize(PublishEntry entry) {
        try {
            metadataClient.finalizePublish(entry.indexName(), true);
            logger.info("[publish-{}] finalizePublish(success=true) completed for [{}]", entry.publishId(), entry.indexName());
            removeEntry(entry);
        } catch (Exception e) {
            handleRetryOnError(entry, "finalizePublish(success=true) failed: " + e.getMessage(), e);
        }
    }
}
