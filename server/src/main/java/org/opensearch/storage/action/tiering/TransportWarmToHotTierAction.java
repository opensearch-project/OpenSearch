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
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport Tiering action to move indices from warm to hot.
 */
public class TransportWarmToHotTierAction extends TransportTierAction {

    /**
     * Constructs a TransportWarmToHotTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param warmToHotTieringService the warm to hot tiering service
     */
    @Inject
    public TransportWarmToHotTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WarmToHotTieringService warmToHotTieringService
    ) {
        super(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            WarmToHotTierAction.NAME,
            warmToHotTieringService
        );
    }
}
