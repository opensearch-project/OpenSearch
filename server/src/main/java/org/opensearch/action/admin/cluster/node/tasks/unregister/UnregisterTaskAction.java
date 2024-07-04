/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.unregister;

import org.opensearch.action.ActionType;

public class UnregisterTaskAction extends ActionType<UnregisterTaskResponse> {
    public static final UnregisterTaskAction INSTANCE = new UnregisterTaskAction();
    public static final String NAME = "cluster:monitor/unregister/task";

    private UnregisterTaskAction() {
        super(NAME, UnregisterTaskResponse::new);
    }

}
