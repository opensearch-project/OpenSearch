/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.filecache.clear;

import org.opensearch.action.ActionType;

/**
 * Transport action for clearing filecache on nodes
 *
 * @opensearch.internal
 */
public class ClearNodesFileCacheAction extends ActionType<ClearNodesFileCacheResponse> {

    public static final ClearNodesFileCacheAction INSTANCE = new ClearNodesFileCacheAction();
    public static final String NAME = "cluster:admin/nodes/filecache/clear";

    private ClearNodesFileCacheAction() {
        super(NAME, ClearNodesFileCacheResponse::new);
    }
}
