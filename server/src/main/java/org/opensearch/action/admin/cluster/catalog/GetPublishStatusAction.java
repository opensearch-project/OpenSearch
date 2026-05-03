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
 * Polls status of an in-flight catalog publish.
 *
 * @opensearch.experimental
 */
public class GetPublishStatusAction extends ActionType<GetPublishStatusResponse> {

    public static final GetPublishStatusAction INSTANCE = new GetPublishStatusAction();
    public static final String NAME = "cluster:monitor/catalog/publish/status";

    private GetPublishStatusAction() {
        super(NAME, GetPublishStatusResponse::new);
    }
}
