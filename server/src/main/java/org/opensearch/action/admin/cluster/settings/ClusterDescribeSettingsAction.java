/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.ActionType;

/** Describes registered cluster settings without reading their values.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsAction extends ActionType<ClusterDescribeSettingsResponse> {
    public static final ClusterDescribeSettingsAction INSTANCE = new ClusterDescribeSettingsAction();
    public static final String NAME = "cluster:monitor/settings/describe";

    private ClusterDescribeSettingsAction() {
        super(NAME, ClusterDescribeSettingsResponse::new);
    }
}
