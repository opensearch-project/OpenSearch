/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.ActionType;

public class CatShardsAction extends ActionType<CatShardsResponse> {
    public static final CatShardsAction INSTANCE = new CatShardsAction();
    public static final String NAME = "cluster:monitor/shards";

    private CatShardsAction() {
        super(NAME, CatShardsResponse::new);
    }
}
