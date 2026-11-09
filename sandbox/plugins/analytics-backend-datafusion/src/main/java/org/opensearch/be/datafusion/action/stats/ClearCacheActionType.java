/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.ActionType;

/**
 * Action type for the broadcast "clear datafusion caches" transport action.
 *
 * <p>The datafusion caches are process-global singletons, one per node. Clearing
 * them must fan out to every node so a single-node REST handler doesn't leave
 * other nodes' caches populated.
 *
 * @opensearch.internal
 */
public class ClearCacheActionType extends ActionType<ClearCacheNodesResponse> {

    public static final String NAME = "cluster:admin/_analytics_backend_datafusion/cache/clear";
    public static final ClearCacheActionType INSTANCE = new ClearCacheActionType();

    private ClearCacheActionType() {
        super(NAME, ClearCacheNodesResponse::new);
    }
}
