/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.action.ActionType;

/**
 * Transport action to prune a named block cache.
 *
 * @opensearch.internal
 */
public class PruneBlockCacheAction extends ActionType<PruneBlockCacheResponse> {

    public static final PruneBlockCacheAction INSTANCE = new PruneBlockCacheAction();
    public static final String NAME = "cluster:admin/blockcache/prune";

    private PruneBlockCacheAction() {
        super(NAME, PruneBlockCacheResponse::new);
    }
}
