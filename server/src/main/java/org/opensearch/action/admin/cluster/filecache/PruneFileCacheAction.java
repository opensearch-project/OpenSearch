/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import org.opensearch.action.ActionType;

/**
 * Transport action to prune file cache
 *
 * @opensearch.internal
 */
public class PruneFileCacheAction extends ActionType<PruneFileCacheResponse> {

    public static final PruneFileCacheAction INSTANCE = new PruneFileCacheAction();
    public static final String NAME = "cluster:admin/filecache/prune";

    private PruneFileCacheAction() {
        super(NAME, PruneFileCacheResponse::new);
    }
}
