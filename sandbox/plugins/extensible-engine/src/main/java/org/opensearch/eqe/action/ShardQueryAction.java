/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.action;

import org.opensearch.action.ActionType;

/**
 * Action type for shard-level plan fragment execution.
 */
public class ShardQueryAction extends ActionType<ShardQueryResponse> {

    public static final ShardQueryAction INSTANCE = new ShardQueryAction();
    public static final String NAME = "indices:data/read/dqe/shard";

    private ShardQueryAction() {
        super(NAME, ShardQueryResponse::new);
    }
}
