/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.tiering.TieringRequests;
import org.opensearch.action.admin.indices.tiering.TieringRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;

import static org.opensearch.action.admin.indices.tiering.TieringUtils.constructToHotToWarmTieringResponse;

public class CompletedTieringStateProcessor extends AbstractTieringStateProcessor {
    private static final Logger logger = LogManager.getLogger(CompletedTieringStateProcessor.class);
    public CompletedTieringStateProcessor(AllocationService allocationService) {
        super(null, allocationService);
    }

    @Override
    public void process(ClusterState clusterState, ClusterService clusterService, TieringRequests tieringRequests) {
        for (TieringRequestContext tieringRequestContext : tieringRequests.getAcceptedTieringRequestContexts()) {
            if (tieringRequestContext.isRequestProcessingComplete()) {
                logger.info("[HotToWarmTiering] Tiering is completed for the request [{}]", tieringRequestContext);
                tieringRequests.getAcceptedTieringRequestContexts().remove(tieringRequestContext);
                if (tieringRequestContext.getListener() != null) {
                    tieringRequestContext.getListener().onResponse(constructToHotToWarmTieringResponse(tieringRequestContext.getFailedIndices()));
                }
            }
        }
    }
}
