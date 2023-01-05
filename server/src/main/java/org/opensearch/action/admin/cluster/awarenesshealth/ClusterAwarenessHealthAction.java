/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.admin.cluster.awarenesshealth;

import org.opensearch.action.ActionType;

/**
 * Transport endpoint action for obtaining cluster awareness_health
 *
 * @opensearch.internal
 */
public class ClusterAwarenessHealthAction extends ActionType<ClusterAwarenessHealthResponse> {

    public static final ClusterAwarenessHealthAction INSTANCE = new ClusterAwarenessHealthAction();
    public static final String NAME = "cluster:monitor/awareness_attribute/health";

    public ClusterAwarenessHealthAction() {
        super(NAME, ClusterAwarenessHealthResponse::new);
    }
}
