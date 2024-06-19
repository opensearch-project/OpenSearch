/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.tasks.CreateTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.tasks.Task;

public class ShardsAction extends ActionType<GetTaskResponse> {
    public static final ShardsAction INSTANCE = new ShardsAction();
    public static final String NAME = "cluster:monitor/create/task";

    private ShardsAction() {
        super(NAME, GetTaskResponse::new);
    }
}
