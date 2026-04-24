/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.ActionType;

/**
 * Action for publishing shard data to an external catalog.
 *
 * @opensearch.experimental
 */
public class PublishShardAction extends ActionType<PublishShardResponse> {

    public static final PublishShardAction INSTANCE = new PublishShardAction();
    public static final String NAME = "indices:admin/catalog/publish_shard";

    private PublishShardAction() {
        super(NAME, PublishShardResponse::new);
    }
}
