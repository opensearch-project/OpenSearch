/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.action.ActionType;

/**
 * Action to fetch metadata from remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreMetadataAction extends ActionType<RemoteStoreMetadataResponse> {
    public static final RemoteStoreMetadataAction INSTANCE = new RemoteStoreMetadataAction();
    public static final String NAME = "cluster:admin/remote_store/metadata";

    private RemoteStoreMetadataAction() {
        super(NAME, RemoteStoreMetadataResponse::new);
    }
}
