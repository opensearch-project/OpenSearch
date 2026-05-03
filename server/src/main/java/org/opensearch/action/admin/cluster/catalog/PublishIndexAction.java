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
 * Async submit for a catalog publish. Returns a correlation id immediately; work is driven
 * by the state machine in {@code RemoteCatalogService}.
 *
 * @opensearch.experimental
 */
public class PublishIndexAction extends ActionType<PublishIndexResponse> {

    public static final PublishIndexAction INSTANCE = new PublishIndexAction();
    public static final String NAME = "cluster:admin/catalog/publish";

    private PublishIndexAction() {
        super(NAME, PublishIndexResponse::new);
    }
}
