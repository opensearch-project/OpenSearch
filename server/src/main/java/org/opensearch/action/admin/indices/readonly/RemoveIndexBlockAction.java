/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.ActionType;

/**
 * Transport action to remove an index block.
 *
 * @opensearch.internal
 */
public class RemoveIndexBlockAction extends ActionType<RemoveIndexBlockResponse> {

    public static final RemoveIndexBlockAction INSTANCE = new RemoveIndexBlockAction();
    public static final String NAME = "indices:admin/block/remove";

    private RemoveIndexBlockAction() {
        super(NAME, RemoveIndexBlockResponse::new);
    }
}
