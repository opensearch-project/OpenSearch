/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.cache;

import org.opensearch.action.ActionType;

/**
 * Transport action to prune file cache
 *
 * @opensearch.internal
 */
public class PruneCacheAction extends ActionType<PruneCacheResponse> {

    public static final PruneCacheAction INSTANCE = new PruneCacheAction();
    public static final String NAME = "cluster:admin/filecache/prune";

    private PruneCacheAction() {
        super(NAME, PruneCacheResponse::new);
    }
}
