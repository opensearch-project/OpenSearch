/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.settings.describe;

import org.opensearch.action.ActionType;

/** Describes registered index settings without reading any index.
 * @opensearch.internal
 */
public class IndicesDescribeSettingsAction extends ActionType<IndicesDescribeSettingsResponse> {
    public static final IndicesDescribeSettingsAction INSTANCE = new IndicesDescribeSettingsAction();
    public static final String NAME = "indices:monitor/settings/describe";

    private IndicesDescribeSettingsAction() {
        super(NAME, IndicesDescribeSettingsResponse::new);
    }
}
