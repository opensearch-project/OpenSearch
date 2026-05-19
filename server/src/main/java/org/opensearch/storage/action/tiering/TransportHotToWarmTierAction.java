/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport Tiering action to move indices from hot to warm.
 */
public class TransportHotToWarmTierAction extends TransportTierAction {

    /**
     * Constructs a TransportHotToWarmTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param hotToWarmTieringService the hot to warm tiering service
     */
    @Inject
    public TransportHotToWarmTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        HotToWarmTieringService hotToWarmTieringService
    ) {
        super(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            HotToWarmTierAction.NAME,
            hotToWarmTieringService
        );
    }
}
